use std::{cmp::Ordering, collections::HashMap};

use datafusion::{
    arrow::{
        array::UInt32Array,
        compute::{concat_batches, take},
        datatypes::Schema as ArrowSchema,
        record_batch::RecordBatch,
    },
    scalar::ScalarValue,
};
use snafu::{Snafu, ensure};
use wings_resources::{PartitionValue, Table, TableName, TableRef};
use wings_schema::{Schema, SchemaBuilder, schema_without_partition_field};

use crate::DeltaLog;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid {operation} schema: expected {expected:?}, got {actual:?}"))]
    InvalidSchema {
        operation: &'static str,
        expected: Box<ArrowSchema>,
        actual: Box<ArrowSchema>,
    },
    #[snafu(display("empty {operation} log"))]
    EmptyLog { operation: &'static str },
    #[snafu(display(
        "cannot merge delta log from a different table: expected {expected}, got {actual}"
    ))]
    TableMismatch {
        expected: TableName,
        actual: TableName,
    },
    #[snafu(display(
        "cannot merge delta log from a different partition: expected {expected:?}, got {actual:?}"
    ))]
    PartitionValueMismatch {
        expected: Option<PartitionValue>,
        actual: Option<PartitionValue>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A collection of delta logs for a specific partition of a table.
#[derive(Debug, Clone)]
pub struct PartitionDeltaLog {
    pub table: TableRef,
    pub partition_value: Option<PartitionValue>,
    pub logs: Vec<DeltaLog>,
}

impl PartitionDeltaLog {
    pub fn new(table: TableRef, partition_value: Option<PartitionValue>) -> Self {
        Self {
            table,
            partition_value,
            logs: Vec::new(),
        }
    }

    pub fn append(&mut self, log: DeltaLog) -> Result<()> {
        let (operation, expected) = match &log {
            DeltaLog::Update(_) => (
                "update",
                schema_without_partition_field(self.table.schema(), self.table.partition_field_id),
            ),
            DeltaLog::Delete(_) => ("delete", deletion_schema(&self.table)),
        };
        let expected = expected.arrow_schema();
        let records = log.records();
        ensure!(records.num_rows() > 0, EmptyLogSnafu { operation });
        let actual = records.schema();

        ensure!(
            actual.as_ref() == &expected,
            InvalidSchemaSnafu {
                operation,
                expected,
                actual: actual.as_ref().clone(),
            }
        );

        self.logs.push(log);

        Ok(())
    }

    pub fn merge(&mut self, other: PartitionDeltaLog) -> Result<()> {
        ensure!(
            self.table.name == other.table.name,
            TableMismatchSnafu {
                expected: self.table.name.clone(),
                actual: other.table.name.clone(),
            }
        );
        ensure!(
            self.partition_value == other.partition_value,
            PartitionValueMismatchSnafu {
                expected: self.partition_value.clone(),
                actual: other.partition_value.clone(),
            }
        );

        self.logs.extend(other.logs);

        Ok(())
    }

    pub fn compacted(&self) -> PartitionDeltaLog {
        let mut rows_by_key = HashMap::new();

        for (log_index, log) in self.logs.iter().enumerate() {
            let records = log.records();
            let schema = records.schema();
            let key_index = field_index(schema.as_ref(), self.table.key_field().name());
            let version_index = field_index(schema.as_ref(), self.table.version_field().name());

            for row_index in 0..records.num_rows() {
                let key = scalar_value(records, key_index, row_index);
                let version = scalar_value(records, version_index, row_index);
                let row = RowRef {
                    log_index,
                    row_index,
                    version,
                };
                let rows = rows_by_key
                    .entry(key)
                    .or_insert_with(CompactedRows::default);
                let current = match log {
                    DeltaLog::Update(_) => &mut rows.update,
                    DeltaLog::Delete(_) => &mut rows.delete,
                };

                if current.as_ref().is_none_or(|current| {
                    version_cmp(&row.version, &current.version) != Ordering::Less
                }) {
                    *current = Some(row);
                }
            }
        }

        let mut update_rows = Vec::new();
        let mut delete_rows = Vec::new();
        for rows in rows_by_key.into_values() {
            match (rows.update, rows.delete) {
                (Some(update), Some(delete)) => match version_cmp(&update.version, &delete.version)
                {
                    Ordering::Greater => update_rows.push(update),
                    Ordering::Less | Ordering::Equal => delete_rows.push(delete),
                },
                (Some(update), None) => update_rows.push(update),
                (None, Some(delete)) => delete_rows.push(delete),
                (None, None) => {}
            }
        }

        update_rows.sort_by_key(|row| (row.log_index, row.row_index));
        delete_rows.sort_by_key(|row| (row.log_index, row.row_index));

        let mut compacted = Self::new(self.table.clone(), self.partition_value.clone());
        if !update_rows.is_empty() {
            compacted
                .append(DeltaLog::Update(crate::DeltaUpdate {
                    records: compact_records(&self.logs, &update_rows),
                }))
                .expect("compacted update log is valid");
        }
        if !delete_rows.is_empty() {
            compacted
                .append(DeltaLog::Delete(crate::DeltaDelete {
                    records: compact_records(&self.logs, &delete_rows),
                }))
                .expect("compacted delete log is valid");
        }

        compacted
    }
}

#[derive(Debug, Default)]
struct CompactedRows {
    update: Option<RowRef>,
    delete: Option<RowRef>,
}

#[derive(Debug)]
struct RowRef {
    log_index: usize,
    row_index: usize,
    version: ScalarValue,
}

fn field_index(schema: &ArrowSchema, field_name: &str) -> usize {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == field_name)
        .expect("delta log schema contains table field")
}

fn scalar_value(records: &RecordBatch, column_index: usize, row_index: usize) -> ScalarValue {
    ScalarValue::try_from_array(records.column(column_index).as_ref(), row_index)
        .expect("record value is valid")
}

fn version_cmp(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    left.partial_cmp(right)
        .expect("version values with the same schema are comparable")
}

fn compact_records(logs: &[DeltaLog], rows: &[RowRef]) -> RecordBatch {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < rows.len() {
        let log_index = rows[offset].log_index;
        let records = logs[log_index].records();
        let mut indices = Vec::new();

        while offset < rows.len() && rows[offset].log_index == log_index {
            indices.push(u32::try_from(rows[offset].row_index).expect("row index fits in u32"));
            offset += 1;
        }

        let indices = UInt32Array::from(indices);
        let columns = records
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None).expect("row selection is valid"))
            .collect::<Vec<_>>();
        batches.push(RecordBatch::try_new(records.schema(), columns).expect("batch is valid"));
    }

    concat_batches(&batches[0].schema(), &batches).expect("compacted batch is valid")
}

pub fn deletion_schema(table: &Table) -> Schema {
    SchemaBuilder::new(vec![
        table.key_field().clone(),
        table.version_field().clone(),
    ])
    .build()
    .expect("derived schema is valid")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{ArrayRef, StringArray, TimestampMillisecondArray},
        record_batch::RecordBatch,
    };
    use wings_resources::{PartitionValue, Table, TableName, TableOptions, TableRef};
    use wings_schema::{
        DataType, Field, Schema, SchemaBuilder, TimeUnit, schema_without_partition_field,
    };

    use super::{Error, PartitionDeltaLog, deletion_schema};
    use crate::{DeltaDelete, DeltaLog, DeltaUpdate, test_util::pretty_format_partition_delta_log};

    fn test_schema() -> Schema {
        SchemaBuilder::new(vec![
            Field::new("key", 0, DataType::Utf8, false),
            Field::new("partition", 2, DataType::Utf8, false),
            Field::new("value", 3, DataType::Utf8, true),
            Field::new("source", 4, DataType::Utf8, true),
            Field::new(
                "update_at",
                1,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])
        .build()
        .unwrap()
    }

    fn test_table() -> TableRef {
        let table_name = TableName::parse("namespaces/my-namespace/tables/my-table").unwrap();
        let options = TableOptions::new(test_schema(), 0, 1).with_partition_field(Some(2));
        Arc::new(Table::new(table_name, options).unwrap())
    }

    fn named_test_table(name: &str) -> TableRef {
        let table_name = TableName::parse(name).unwrap();
        let options = TableOptions::new(test_schema(), 0, 1).with_partition_field(Some(2));
        Arc::new(Table::new(table_name, options).unwrap())
    }

    fn record_batch(schema: Schema) -> RecordBatch {
        let mut columns: Vec<ArrayRef> = Vec::new();
        for field in schema.fields_iter() {
            match field.id {
                0 => columns.push(Arc::new(StringArray::from(vec!["a"]))),
                1 => columns.push(Arc::new(TimestampMillisecondArray::from(vec![1]))),
                2 => columns.push(Arc::new(StringArray::from(vec!["p"]))),
                3 => columns.push(Arc::new(StringArray::from(vec![Some("value")]))),
                4 => columns.push(Arc::new(StringArray::from(vec![Some("source")]))),
                _ => unreachable!("unexpected test field"),
            }
        }

        RecordBatch::try_new(Arc::new(schema.arrow_schema()), columns).unwrap()
    }

    fn update_records(table: &Table, rows: &[(&str, &str, &str, i64)]) -> RecordBatch {
        let schema = schema_without_partition_field(table.schema(), table.partition_field_id);
        RecordBatch::try_new(
            Arc::new(schema.arrow_schema()),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMillisecondArray::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn delete_records(table: &Table, rows: &[(&str, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(deletion_schema(table).arrow_schema()),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMillisecondArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn append_accepts_update_schema_without_partition_field() {
        let table = test_table();
        let records = record_batch(schema_without_partition_field(
            table.schema(),
            table.partition_field_id,
        ));

        let mut delta_log = PartitionDeltaLog::new(table, None);

        delta_log
            .append(DeltaLog::Update(DeltaUpdate { records }))
            .unwrap();

        assert_eq!(delta_log.logs.len(), 1);
    }

    #[test]
    fn append_rejects_update_schema_with_partition_field() {
        let table = test_table();
        let records = record_batch(table.schema().clone());
        let mut delta_log = PartitionDeltaLog::new(table, None);

        let error = delta_log
            .append(DeltaLog::Update(DeltaUpdate { records }))
            .unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidSchema {
                operation: "update",
                ..
            }
        ));
        assert!(delta_log.logs.is_empty());
    }

    #[test]
    fn append_rejects_empty_update_log() {
        let table = test_table();
        let schema = schema_without_partition_field(table.schema(), table.partition_field_id);
        let records = RecordBatch::new_empty(Arc::new(schema.arrow_schema()));
        let mut delta_log = PartitionDeltaLog::new(table, None);

        let error = delta_log
            .append(DeltaLog::Update(DeltaUpdate { records }))
            .unwrap_err();

        assert!(matches!(
            error,
            Error::EmptyLog {
                operation: "update"
            }
        ));
        assert!(delta_log.logs.is_empty());
    }

    #[test]
    fn append_accepts_delete_schema() {
        let table = test_table();
        let records = record_batch(deletion_schema(&table));
        let mut delta_log = PartitionDeltaLog::new(table, None);

        delta_log
            .append(DeltaLog::Delete(DeltaDelete { records }))
            .unwrap();

        assert_eq!(delta_log.logs.len(), 1);
    }

    #[test]
    fn append_rejects_delete_schema_with_extra_fields() {
        let table = test_table();
        let records = record_batch(schema_without_partition_field(
            table.schema(),
            table.partition_field_id,
        ));
        let mut delta_log = PartitionDeltaLog::new(table, None);

        let error = delta_log
            .append(DeltaLog::Delete(DeltaDelete { records }))
            .unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidSchema {
                operation: "delete",
                ..
            }
        ));
        assert!(delta_log.logs.is_empty());
    }

    #[test]
    fn deletion_schema_returns_key_and_version_fields() {
        let table = test_table();
        let schema = deletion_schema(&table);
        let field_names = schema
            .fields_iter()
            .map(|field| field.name())
            .collect::<Vec<_>>();

        assert_eq!(field_names, vec!["key", "update_at"]);
    }

    #[test]
    fn merge_appends_logs_for_the_same_table_and_partition_value() {
        let table = test_table();
        let partition_value = Some(PartitionValue::String("partition-a".to_string()));

        let mut left = PartitionDeltaLog::new(table.clone(), partition_value.clone());
        left.append(DeltaLog::Update(DeltaUpdate {
            records: update_records(&table, &[("a", "left-a", "api", 1_000)]),
        }))
        .unwrap();

        let mut right = PartitionDeltaLog::new(table.clone(), partition_value);
        right
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(&table, &[("b", 2_000)]),
            }))
            .unwrap();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&left).unwrap(), @r"
        ┌─────────────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table         │
        │ partition-a                                     │
        ├─────────────────────────────────────────────────┤
        │ ┌─Δ update─────┬────────┬─────────────────────┐ │
        │ │ key │ value  │ source │ update_at           │ │
        │ ├─────┼────────┼────────┼─────────────────────┤ │
        │ │ a   │ left-a │ api    │ 1970-01-01T00:00:01 │ │
        │ └─────┴────────┴────────┴─────────────────────┘ │
        └─────────────────────────────────────────────────┘
        ");
        insta::assert_snapshot!(pretty_format_partition_delta_log(&right).unwrap(), @r"
        ┌─────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table │
        │ partition-a                             │
        ├─────────────────────────────────────────┤
        │ ┌─Δ delete──────────────────┐           │
        │ │ key │ update_at           │           │
        │ ├─────┼─────────────────────┤           │
        │ │ b   │ 1970-01-01T00:00:02 │           │
        │ └─────┴─────────────────────┘           │
        └─────────────────────────────────────────┘
        ");

        left.merge(right).unwrap();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&left).unwrap(), @r"
        ┌─────────────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table         │
        │ partition-a                                     │
        ├─────────────────────────────────────────────────┤
        │ ┌─Δ update─────┬────────┬─────────────────────┐ │
        │ │ key │ value  │ source │ update_at           │ │
        │ ├─────┼────────┼────────┼─────────────────────┤ │
        │ │ a   │ left-a │ api    │ 1970-01-01T00:00:01 │ │
        │ └─────┴────────┴────────┴─────────────────────┘ │
        │ ┌─Δ delete──────────────────┐                   │
        │ │ key │ update_at           │                   │
        │ ├─────┼─────────────────────┤                   │
        │ │ b   │ 1970-01-01T00:00:02 │                   │
        │ └─────┴─────────────────────┘                   │
        └─────────────────────────────────────────────────┘
        ");
    }

    #[test]
    fn merge_rejects_different_tables() {
        let table = test_table();
        let other_table = named_test_table("namespaces/my-namespace/tables/other-table");
        let mut left = PartitionDeltaLog::new(table, None);
        let right = PartitionDeltaLog::new(other_table, None);

        let error = left.merge(right).unwrap_err();

        assert!(matches!(error, Error::TableMismatch { .. }));
    }

    #[test]
    fn merge_rejects_different_partition_values() {
        let table = test_table();
        let mut left = PartitionDeltaLog::new(
            table.clone(),
            Some(PartitionValue::String("partition-a".to_string())),
        );
        let right = PartitionDeltaLog::new(
            table,
            Some(PartitionValue::String("partition-b".to_string())),
        );

        let error = left.merge(right).unwrap_err();

        assert!(matches!(error, Error::PartitionValueMismatch { .. }));
    }

    #[test]
    fn compacted_keeps_only_latest_mutation_per_key() {
        let table = test_table();
        let mut delta_log = PartitionDeltaLog::new(table.clone(), None);
        delta_log
            .append(DeltaLog::Update(DeltaUpdate {
                records: update_records(
                    &table,
                    &[
                        ("a", "old-a", "api", 1_000),
                        ("a", "new-a", "api", 3_000),
                        ("b", "old-b", "api", 2_000),
                        ("c", "update-c", "worker", 5_000),
                    ],
                ),
            }))
            .unwrap();
        delta_log
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(
                    &table,
                    &[("a", 2_000), ("b", 3_000), ("c", 5_000), ("d", 1_000)],
                ),
            }))
            .unwrap();
        delta_log
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(&table, &[("d", 2_000)]),
            }))
            .unwrap();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&delta_log).unwrap(), @r"
        ┌───────────────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table           │
        │ ∅                                                 │
        ├───────────────────────────────────────────────────┤
        │ ┌─Δ update───────┬────────┬─────────────────────┐ │
        │ │ key │ value    │ source │ update_at           │ │
        │ ├─────┼──────────┼────────┼─────────────────────┤ │
        │ │ a   │ old-a    │ api    │ 1970-01-01T00:00:01 │ │
        │ │ a   │ new-a    │ api    │ 1970-01-01T00:00:03 │ │
        │ │ b   │ old-b    │ api    │ 1970-01-01T00:00:02 │ │
        │ │ c   │ update-c │ worker │ 1970-01-01T00:00:05 │ │
        │ └─────┴──────────┴────────┴─────────────────────┘ │
        │ ┌─Δ delete──────────────────┐                     │
        │ │ key │ update_at           │                     │
        │ ├─────┼─────────────────────┤                     │
        │ │ a   │ 1970-01-01T00:00:02 │                     │
        │ │ b   │ 1970-01-01T00:00:03 │                     │
        │ │ c   │ 1970-01-01T00:00:05 │                     │
        │ │ d   │ 1970-01-01T00:00:01 │                     │
        │ └─────┴─────────────────────┘                     │
        │ ┌─Δ delete──────────────────┐                     │
        │ │ key │ update_at           │                     │
        │ ├─────┼─────────────────────┤                     │
        │ │ d   │ 1970-01-01T00:00:02 │                     │
        │ └─────┴─────────────────────┘                     │
        └───────────────────────────────────────────────────┘
        ");

        let compacted = delta_log.compacted();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&compacted).unwrap(), @r"
        ┌────────────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table        │
        │ ∅                                              │
        ├────────────────────────────────────────────────┤
        │ ┌─Δ update────┬────────┬─────────────────────┐ │
        │ │ key │ value │ source │ update_at           │ │
        │ ├─────┼───────┼────────┼─────────────────────┤ │
        │ │ a   │ new-a │ api    │ 1970-01-01T00:00:03 │ │
        │ └─────┴───────┴────────┴─────────────────────┘ │
        │ ┌─Δ delete──────────────────┐                  │
        │ │ key │ update_at           │                  │
        │ ├─────┼─────────────────────┤                  │
        │ │ b   │ 1970-01-01T00:00:03 │                  │
        │ │ c   │ 1970-01-01T00:00:05 │                  │
        │ │ d   │ 1970-01-01T00:00:02 │                  │
        │ └─────┴─────────────────────┘                  │
        └────────────────────────────────────────────────┘
        ");
    }

    #[test]
    fn compacted_can_have_no_updates() {
        let table = test_table();
        let mut delta_log = PartitionDeltaLog::new(table.clone(), None);
        delta_log
            .append(DeltaLog::Update(DeltaUpdate {
                records: update_records(
                    &table,
                    &[("a", "old-a", "api", 1_000), ("b", "old-b", "api", 2_000)],
                ),
            }))
            .unwrap();
        delta_log
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(&table, &[("a", 1_000), ("b", 3_000)]),
            }))
            .unwrap();

        let compacted = delta_log.compacted();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&compacted).unwrap(), @r"
        ┌─────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table │
        │ ∅                                       │
        ├─────────────────────────────────────────┤
        │ ┌─Δ delete──────────────────┐           │
        │ │ key │ update_at           │           │
        │ ├─────┼─────────────────────┤           │
        │ │ a   │ 1970-01-01T00:00:01 │           │
        │ │ b   │ 1970-01-01T00:00:03 │           │
        │ └─────┴─────────────────────┘           │
        └─────────────────────────────────────────┘
        ");
    }

    #[test]
    fn compacted_can_have_no_deletes() {
        let table = test_table();
        let mut delta_log = PartitionDeltaLog::new(table.clone(), None);
        delta_log
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(&table, &[("a", 1_000), ("b", 2_000)]),
            }))
            .unwrap();
        delta_log
            .append(DeltaLog::Update(DeltaUpdate {
                records: update_records(
                    &table,
                    &[
                        ("a", "new-a", "api", 2_000),
                        ("b", "new-b", "worker", 3_000),
                    ],
                ),
            }))
            .unwrap();

        let compacted = delta_log.compacted();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&compacted).unwrap(), @r"
        ┌────────────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table        │
        │ ∅                                              │
        ├────────────────────────────────────────────────┤
        │ ┌─Δ update────┬────────┬─────────────────────┐ │
        │ │ key │ value │ source │ update_at           │ │
        │ ├─────┼───────┼────────┼─────────────────────┤ │
        │ │ a   │ new-a │ api    │ 1970-01-01T00:00:02 │ │
        │ │ b   │ new-b │ worker │ 1970-01-01T00:00:03 │ │
        │ └─────┴───────┴────────┴─────────────────────┘ │
        └────────────────────────────────────────────────┘
        ");
    }

    #[test]
    fn compacted_delete_wins_when_versions_are_equal() {
        let table = test_table();

        let mut update_then_delete = PartitionDeltaLog::new(table.clone(), None);
        update_then_delete
            .append(DeltaLog::Update(DeltaUpdate {
                records: update_records(&table, &[("a", "value-a", "api", 1_000)]),
            }))
            .unwrap();
        update_then_delete
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(&table, &[("a", 1_000)]),
            }))
            .unwrap();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&update_then_delete.compacted()).unwrap(), @r"
        ┌─────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table │
        │ ∅                                       │
        ├─────────────────────────────────────────┤
        │ ┌─Δ delete──────────────────┐           │
        │ │ key │ update_at           │           │
        │ ├─────┼─────────────────────┤           │
        │ │ a   │ 1970-01-01T00:00:01 │           │
        │ └─────┴─────────────────────┘           │
        └─────────────────────────────────────────┘
        ");

        let mut delete_then_update = PartitionDeltaLog::new(table.clone(), None);
        delete_then_update
            .append(DeltaLog::Delete(DeltaDelete {
                records: delete_records(&table, &[("a", 1_000)]),
            }))
            .unwrap();
        delete_then_update
            .append(DeltaLog::Update(DeltaUpdate {
                records: update_records(&table, &[("a", "value-a", "api", 1_000)]),
            }))
            .unwrap();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&delete_then_update.compacted()).unwrap(), @r"
        ┌─────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table │
        │ ∅                                       │
        ├─────────────────────────────────────────┤
        │ ┌─Δ delete──────────────────┐           │
        │ │ key │ update_at           │           │
        │ ├─────┼─────────────────────┤           │
        │ │ a   │ 1970-01-01T00:00:01 │           │
        │ └─────┴─────────────────────┘           │
        └─────────────────────────────────────────┘
        ");
    }
}
