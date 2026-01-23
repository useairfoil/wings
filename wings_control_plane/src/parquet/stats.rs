// This module is inspired by the parquet writer in the iceberg-rust crate.
//
// https://github.com/apache/iceberg-rust/blob/2944ccb/crates/iceberg/src/writer/file_writer/parquet_writer.rs
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use bytesize::ByteSize;
use parquet::file::{metadata::ParquetMetaData, statistics::Statistics};

use crate::{
    parquet::parquet_name_index::create_parquet_path_index,
    schema::{DataType, Datum, SchemaRef},
};

use super::error::Result;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub file_size: bytesize::ByteSize,
    pub num_rows: usize,
    pub column_sizes: HashMap<u64, u64>,
    pub value_counts: HashMap<u64, u64>,
    pub null_value_counts: HashMap<u64, u64>,
    pub lower_bounds: HashMap<u64, Datum>,
    pub upper_bounds: HashMap<u64, Datum>,
}

pub fn parquet_metadata_to_file_metadata(
    schema: SchemaRef,
    file_size: ByteSize,
    metadata: ParquetMetaData,
) -> Result<FileMetadata> {
    let index_by_parquet_path = create_parquet_path_index(schema.as_ref())?;

    let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
        let mut per_col_size: HashMap<u64, u64> = HashMap::new();
        let mut per_col_val_num: HashMap<u64, u64> = HashMap::new();
        let mut per_col_null_val_num: HashMap<u64, u64> = HashMap::new();
        let mut min_max_agg = MinMaxColAggregator::new(schema);

        for row_group in metadata.row_groups() {
            for column_chunk_metadata in row_group.columns() {
                let parquet_path = column_chunk_metadata.column_descr().path().string();

                let Some(&field_id) = index_by_parquet_path.get(&parquet_path) else {
                    continue;
                };

                *per_col_size.entry(field_id).or_insert(0) +=
                    column_chunk_metadata.compressed_size() as u64;
                *per_col_val_num.entry(field_id).or_insert(0) +=
                    column_chunk_metadata.num_values() as u64;

                if let Some(statistics) = column_chunk_metadata.statistics() {
                    if let Some(null_count) = statistics.null_count_opt() {
                        *per_col_null_val_num.entry(field_id).or_insert(0) += null_count;
                    }

                    min_max_agg.update(field_id, statistics.clone())?;
                }
            }
        }

        (
            per_col_size,
            per_col_val_num,
            per_col_null_val_num,
            min_max_agg.produce(),
        )
    };

    Ok(FileMetadata {
        file_size,
        num_rows: metadata.file_metadata().num_rows() as _,
        column_sizes,
        value_counts,
        null_value_counts,
        lower_bounds,
        upper_bounds,
    })
}

/// Used to aggregate min and max value of each column.
struct MinMaxColAggregator {
    schema: SchemaRef,
    lower_bounds: HashMap<u64, Datum>,
    upper_bounds: HashMap<u64, Datum>,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
        }
    }

    fn update_state_min(&mut self, field_id: u64, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    fn update_state_max(&mut self, field_id: u64, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e < datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
    fn update(&mut self, field_id: u64, value: Statistics) -> Result<()> {
        let Some(field) = self.schema.field_by_id(field_id).cloned() else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };

        let ty = field.data_type();

        if !ty.is_primitive_type() {
            return Ok(());
        }

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(ty, &value)? else {
                return Err(super::error::Error::InvalidStatisticsType {
                    field_id: field.id,
                    field_name: field.name().to_string(),
                });
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(ty, &value)? else {
                return Err(super::error::Error::InvalidStatisticsType {
                    field_id: field.id,
                    field_name: field.name().to_string(),
                });
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    fn produce(self) -> (HashMap<u64, Datum>, HashMap<u64, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

fn get_parquet_stat_min_as_datum(
    data_type: &DataType,
    stats: &Statistics,
) -> Result<Option<Datum>> {
    Ok(match (data_type, stats) {
        (DataType::Null, _) => None,
        (DataType::Boolean, Statistics::Boolean(stats)) => {
            stats.min_opt().map(|val| Datum::bool(*val))
        }
        (DataType::Int8, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::i8(*val as _))
        }
        (DataType::Int16, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::i16(*val as _))
        }
        (DataType::Int32, Statistics::Int32(stats)) => stats.min_opt().map(|val| Datum::i32(*val)),
        (DataType::Int64, Statistics::Int64(stats)) => stats.min_opt().map(|val| Datum::i64(*val)),
        (DataType::UInt8, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::u8(*val as _))
        }
        (DataType::UInt16, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::u16(*val as _))
        }
        (DataType::UInt32, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::u32(*val as _))
        }
        (DataType::UInt64, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::u64(*val as _))
        }
        (DataType::Float16, Statistics::Float(_stats)) => {
            // TODO: check how it's represented
            None
        }
        (DataType::Float32, Statistics::Float(stats)) => {
            stats.min_opt().map(|val| Datum::f32(*val))
        }
        (DataType::Float64, Statistics::Double(stats)) => {
            stats.min_opt().map(|val| Datum::f64(*val))
        }
        (DataType::Timestamp(unit, _), Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp(*unit, *val))
        }
        (DataType::Duration(unit), Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::duration(*unit, *val))
        }

        (DataType::Date32, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::date32(*val))
        }
        (DataType::Date64, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::date64(*val))
        }

        (DataType::Binary, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.min_bytes_opt() else {
                return Ok(None);
            };

            Some(Datum::binary(val.to_vec()))
        }

        (DataType::Utf8, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::utf8(val.as_utf8()?))
        }

        _ => None,
    })
}

fn get_parquet_stat_max_as_datum(
    data_type: &DataType,
    stats: &Statistics,
) -> Result<Option<Datum>> {
    Ok(match (data_type, stats) {
        (DataType::Null, _) => None,
        (DataType::Boolean, Statistics::Boolean(stats)) => {
            stats.max_opt().map(|val| Datum::bool(*val))
        }
        (DataType::Int8, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::i8(*val as _))
        }
        (DataType::Int16, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::i16(*val as _))
        }
        (DataType::Int32, Statistics::Int32(stats)) => stats.max_opt().map(|val| Datum::i32(*val)),
        (DataType::Int64, Statistics::Int64(stats)) => stats.max_opt().map(|val| Datum::i64(*val)),
        (DataType::UInt8, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::u8(*val as _))
        }
        (DataType::UInt16, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::u16(*val as _))
        }
        (DataType::UInt32, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::u32(*val as _))
        }
        (DataType::UInt64, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::u64(*val as _))
        }
        (DataType::Float16, Statistics::Float(_stats)) => {
            // TODO: check how it's represented
            None
        }
        (DataType::Float32, Statistics::Float(stats)) => {
            stats.max_opt().map(|val| Datum::f32(*val))
        }
        (DataType::Float64, Statistics::Double(stats)) => {
            stats.max_opt().map(|val| Datum::f64(*val))
        }
        (DataType::Timestamp(unit, _), Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp(*unit, *val))
        }
        (DataType::Duration(unit), Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::duration(*unit, *val))
        }

        (DataType::Date32, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::date32(*val))
        }
        (DataType::Date64, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::date64(*val))
        }

        (DataType::Binary, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.max_bytes_opt() else {
                return Ok(None);
            };

            Some(Datum::binary(val.to_vec()))
        }

        (DataType::Utf8, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::utf8(val.as_utf8()?))
        }

        _ => None,
    })
}
