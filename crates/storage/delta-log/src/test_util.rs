use arrow_cast::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use snafu::{ResultExt, Snafu};
use tabled::{
    Table,
    settings::{
        Style,
        object::Rows,
        style::{HorizontalLine, LineText},
    },
};

use crate::{DeltaLog, partition::PartitionDeltaLog};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid argument: {}", message))]
    InvalidArgument { message: String },
    #[snafu(display("Arrow formatter error"))]
    Formatter { source: ArrowError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn pretty_format_partition_delta_log(pdl: &PartitionDeltaLog) -> Result<Table> {
    let mut cells = vec![
        pdl.table.name.to_string(),
        pdl.partition_value
            .as_ref()
            .map(|pv| pv.to_string())
            .unwrap_or("∅".to_string()),
    ];

    for log in &pdl.logs {
        let log_table = pretty_format_delta_log(log)?;
        cells.push(log_table.to_string());
    }

    let mut builder = tabled::builder::Builder::default();
    builder.push_column(cells);
    let mut table = builder.build();
    table.with(
        Style::sharp()
            .remove_horizontals()
            .horizontals([(2, HorizontalLine::inherit(Style::modern()))]),
    );

    Ok(table)
}

pub fn pretty_format_delta_log(log: &DeltaLog) -> Result<Table> {
    let mut table = create_record_table(None, log.records())?;
    let op = match log {
        DeltaLog::Update(_) => "Δ update",
        DeltaLog::Delete(_) => "Δ delete",
    };

    table
        .with(Style::sharp())
        .with(LineText::new(op, Rows::first()).offset(2));

    Ok(table)
}

/// Write the records to a comfy table.
///
/// This is taken from arrow_cast::pretty and adapted for our internal use.
fn create_record_table(schema: Option<SchemaRef>, batch: &RecordBatch) -> Result<Table> {
    use tabled::builder::Builder;
    let mut builder = Builder::default();

    let formatter_options = FormatOptions::default();

    let schema = schema.unwrap_or_else(|| batch.schema());

    // Could be a custom schema that was provided.
    if batch.columns().len() != schema.fields().len() {
        return InvalidArgumentSnafu {
                message: format!(
                    "Expected the same number of columns in a record batch ({}) as the number of fields ({}) in the schema",
                    batch.columns().len(),
                    schema.fields.len()
                ),
            }.fail();
    }

    let formatters = batch
        .columns()
        .iter()
        .map(|col| ArrayFormatter::try_new(col, &formatter_options))
        .collect::<Result<Vec<_>, ArrowError>>()
        .context(FormatterSnafu {})?;

    for (formatter, field) in formatters.into_iter().zip(schema.fields().iter()) {
        let mut cells = vec![field.name().clone()];
        for row in 0..batch.num_rows() {
            cells.push(formatter.value(row).to_string());
        }
        builder.push_column(cells);
    }

    let table = builder.build();

    Ok(table)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{self, RecordBatch};
    use wings_resources::{Table, TableName, TableOptions, TableRef};
    use wings_schema::{DataType, Field, Schema, SchemaBuilder};

    use crate::{
        DeltaDelete, DeltaLog, DeltaUpdate,
        partition::PartitionDeltaLog,
        test_util::{pretty_format_delta_log, pretty_format_partition_delta_log},
    };

    fn test_schema() -> Schema {
        SchemaBuilder::new(vec![
            Field::new("key", 0, DataType::Utf8, false),
            Field::new("version", 1, DataType::Int32, false),
        ])
        .build()
        .unwrap()
    }

    fn test_table() -> TableRef {
        let table_name = TableName::parse("namespaces/my-namespace/tables/my-table").unwrap();
        let options = TableOptions::new(test_schema(), 0, 1);
        let table = Table::new(table_name, options).unwrap();
        Arc::new(table)
    }

    fn test_data() -> RecordBatch {
        let schema: Arc<_> = test_schema().arrow_schema().into();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(array::StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    Some("d"),
                ])),
                Arc::new(array::Int32Array::from(vec![
                    Some(1),
                    Some(3),
                    Some(10),
                    Some(100),
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_pretty_format_empty_records() {
        let schema: Arc<_> = test_schema().arrow_schema().into();

        let records = RecordBatch::new_empty(schema);

        let delta_log = DeltaLog::Update(DeltaUpdate { records });

        insta::assert_snapshot!(pretty_format_delta_log(&delta_log).unwrap(), @r"
        ┌─Δ update──────┐
        │ key │ version │
        ├─────┼─────────┤
        ");
    }

    #[test]
    fn test_pretty_format_delta_update() {
        let delta_log = DeltaLog::Update(DeltaUpdate {
            records: test_data(),
        });

        insta::assert_snapshot!(pretty_format_delta_log(&delta_log).unwrap(), @r"
        ┌─Δ update──────┐
        │ key │ version │
        ├─────┼─────────┤
        │ a   │ 1       │
        │ b   │ 3       │
        │ b   │ 10      │
        │ d   │ 100     │
        └─────┴─────────┘
        ");
    }

    #[test]
    fn test_pretty_format_delta_delete() {
        let delta_log = DeltaLog::Delete(DeltaDelete {
            records: test_data(),
        });

        insta::assert_snapshot!(pretty_format_delta_log(&delta_log).unwrap(), @r"
        ┌─Δ delete──────┐
        │ key │ version │
        ├─────┼─────────┤
        │ a   │ 1       │
        │ b   │ 3       │
        │ b   │ 10      │
        │ d   │ 100     │
        └─────┴─────────┘
        ");
    }

    #[test]
    fn test_pretty_format_partition_delta_log() {
        let table = test_table();
        let mut pv_log = PartitionDeltaLog::new(table, None);
        pv_log
            .append(DeltaLog::Update(DeltaUpdate {
                records: test_data(),
            }))
            .unwrap();
        pv_log
            .append(DeltaLog::Delete(DeltaDelete {
                records: test_data(),
            }))
            .unwrap();

        insta::assert_snapshot!(pretty_format_partition_delta_log(&pv_log).unwrap(), @r"
        ┌─────────────────────────────────────────┐
        │ namespaces/my-namespace/tables/my-table │
        │ ∅                                       │
        ├─────────────────────────────────────────┤
        │ ┌─Δ update──────┐                       │
        │ │ key │ version │                       │
        │ ├─────┼─────────┤                       │
        │ │ a   │ 1       │                       │
        │ │ b   │ 3       │                       │
        │ │ b   │ 10      │                       │
        │ │ d   │ 100     │                       │
        │ └─────┴─────────┘                       │
        │ ┌─Δ delete──────┐                       │
        │ │ key │ version │                       │
        │ ├─────┼─────────┤                       │
        │ │ a   │ 1       │                       │
        │ │ b   │ 3       │                       │
        │ │ b   │ 10      │                       │
        │ │ d   │ 100     │                       │
        │ └─────┴─────────┘                       │
        └─────────────────────────────────────────┘
        ");
    }
}
