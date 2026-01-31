// This code originally comes from the DataFusion project.
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

use std::{collections::HashMap, hash::Hash, sync::Arc};

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, TimeUnit as ArrowTimeUnit,
};

use crate::{Field, FieldRef, Fields};

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimeUnit {
    /// Time in seconds.
    Second,
    /// Time in milliseconds.
    Millisecond,
    /// Time in microseconds.
    Microsecond,
    /// Time in nanoseconds.
    Nanosecond,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a signed 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// Timestamps with a non-empty timezone
    /// ------------------------------------
    ///
    /// If a Timestamp column has a non-empty timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
    /// (the Unix epoch), regardless of the Timestamp's own timezone.
    ///
    /// Therefore, timestamp values with a non-empty timezone correspond to
    /// physical points in time together with some additional information about
    /// how the data was obtained and/or how to display it (the timezone).
    ///
    ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
    ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
    ///   application may prefer to display it as "January 1st 1970, 01h00" in
    ///   the Europe/Paris timezone (which is the same physical point in time).
    ///
    /// One consequence is that timestamp values with a non-empty timezone
    /// can be compared and ordered directly, since they all share the same
    /// well-known point of reference (the Unix epoch).
    ///
    /// Timestamps with an unset / empty timezone
    /// -----------------------------------------
    ///
    /// If a Timestamp column has no timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
    ///
    /// Therefore, timestamp values without a timezone cannot be meaningfully
    /// interpreted as physical points in time, but only as calendar / clock
    /// indications ("wall clock time") in an unspecified timezone.
    ///
    ///   For example, the timestamp value 0 with an empty timezone string
    ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
    ///   is not enough information to interpret it as a well-defined physical
    ///   point in time.
    ///
    /// One consequence is that timestamp values without a timezone cannot
    /// be reliably compared or ordered, since they may have different points of
    /// reference.  In particular, it is *not* possible to interpret an unset
    /// or empty timezone as the same as "UTC".
    ///
    /// Conversion between timezones
    /// ----------------------------
    ///
    /// If a Timestamp column has a non-empty timezone, changing the timezone
    /// to a different non-empty value is a metadata-only operation:
    /// the timestamp values need not change as their point of reference remains
    /// the same (the Unix epoch).
    ///
    /// However, if a Timestamp column has no timezone value, changing it to a
    /// non-empty value requires to think about the desired semantics.
    /// One possibility is to assume that the original timestamp values are
    /// relative to the epoch of the timezone being set; timestamp values should
    /// then adjusted to the Unix epoch (for example, changing the timezone from
    /// empty to "Europe/Paris" would require converting the timestamp values
    /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
    /// nevertheless correct).
    ///
    /// ```
    /// # use wings_control_plane::schema::{DataType, TimeUnit};
    /// DataType::Timestamp(TimeUnit::Second, None);
    /// DataType::Timestamp(TimeUnit::Second, Some("literal".into()));
    /// DataType::Timestamp(TimeUnit::Second, Some("string".to_string().into()));
    /// ```
    ///
    /// # Timezone representation
    /// ----------------------------
    /// It is possible to use either the timezone string representation, such as "UTC", or the absolute time zone offset "+00:00".
    /// For timezones with fixed offsets, such as "UTC" or "JST", the offset representation is recommended, as it is more explicit and less ambiguous.
    ///
    /// Most arrow-rs functionalities use the absolute offset representation,
    /// such as [`PrimitiveArray::with_timezone_utc`] that applies a
    /// UTC timezone to timestamp arrays.
    ///
    /// [`PrimitiveArray::with_timezone_utc`]: https://docs.rs/arrow/latest/arrow/array/struct.PrimitiveArray.html#method.with_timezone_utc
    ///
    /// Timezone string parsing
    /// -----------------------
    /// When feature `chrono-tz` is not enabled, allowed timezone strings are fixed offsets of the form "+09:00", "-09" or "+0930".
    ///
    /// When feature `chrono-tz` is enabled, additional strings supported by [chrono_tz](https://docs.rs/chrono-tz/latest/chrono_tz/)
    /// are also allowed, which include [IANA database](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
    /// timezones.
    Timestamp(TimeUnit, Option<Arc<str>>),
    /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds.
    ///
    /// # Valid Ranges
    ///
    /// According to the Arrow specification ([Schema.fbs]), values of Date64
    /// are treated as the number of *days*, in milliseconds, since the UNIX
    /// epoch. Therefore, values of this type  must be evenly divisible by
    /// `86_400_000`, the number of milliseconds in a standard day.
    ///
    /// It is not valid to store milliseconds that do not represent an exact
    /// day. The reason for this restriction is compatibility with other
    /// language's native libraries (specifically Java), which historically
    /// lacked a dedicated date type and only supported timestamps.
    ///
    /// # Validation
    ///
    /// This library does not validate or enforce that Date64 values are evenly
    /// divisible by `86_400_000`  for performance and usability reasons. Date64
    /// values are treated similarly to `Timestamp(TimeUnit::Millisecond,
    /// None)`: values will be displayed with a time of day if the value does
    /// not represent an exact day, and arithmetic will be done at the
    /// millisecond granularity.
    ///
    /// # Recommendation
    ///
    /// Users should prefer [`Date32`] to cleanly represent the number
    /// of days, or one of the Timestamp variants to include time as part of the
    /// representation, depending on their use case.
    ///
    /// # Further Reading
    ///
    /// For more details, see [#5288](https://github.com/apache/arrow-rs/issues/5288).
    ///
    /// [`Date32`]: Self::Date32
    /// [Schema.fbs]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
    Date64,
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// Opaque binary data of variable length.
    ///
    /// A single Binary array can store up to [`i32::MAX`] bytes
    /// of binary data in total.
    Binary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    ///
    /// A single Utf8 array can store up to [`i32::MAX`] bytes
    /// of string data in total.
    Utf8,
    /// A list of some logical data type with variable length.
    ///
    /// A single List array can store up to [`i32::MAX`] elements in total.
    List(FieldRef),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Fields),
}

impl DataType {
    pub fn is_primitive_type(&self) -> bool {
        !matches!(self, Self::List(_) | Self::Struct(_))
    }
}

impl From<DataType> for ArrowDataType {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Null => ArrowDataType::Null,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float16 => ArrowDataType::Float16,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Timestamp(unit, timezone) => ArrowDataType::Timestamp(unit.into(), timezone),
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::Duration(unit) => ArrowDataType::Duration(unit.into()),
            DataType::Binary => ArrowDataType::Binary,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::List(field) => {
                let arrow_field = Arc::try_unwrap(field)
                    .unwrap_or_else(|f| f.as_ref().clone())
                    .into_arrow_field();
                ArrowDataType::List(Arc::new(arrow_field))
            }
            DataType::Struct(fields) => {
                let arrow_fields: Vec<ArrowField> =
                    fields.iter().map(|f| f.to_arrow_field()).collect();
                ArrowDataType::Struct(arrow_fields.into())
            }
        }
    }
}

impl From<TimeUnit> for ArrowTimeUnit {
    fn from(tu: TimeUnit) -> Self {
        match tu {
            TimeUnit::Second => ArrowTimeUnit::Second,
            TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
            TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
            TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
        }
    }
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Second => write!(f, "s"),
            TimeUnit::Millisecond => write!(f, "ms"),
            TimeUnit::Microsecond => write!(f, "µs"),
            TimeUnit::Nanosecond => write!(f, "ns"),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn format_metadata(metadata: &HashMap<String, String>) -> String {
            format!("{}", FormatMetadata(metadata))
        }

        fn format_nullability(field: &Field) -> &str {
            if field.is_nullable() { "" } else { "non-null " }
        }

        fn format_field(field: &Field) -> String {
            let name = field.name();
            let maybe_nullable = format_nullability(field);
            let data_type = field.data_type();
            let metadata_str = format_metadata(field.metadata());
            format!("{name:?}: {maybe_nullable}{data_type}{metadata_str}")
        }

        match &self {
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Float16 => write!(f, "Float16"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Timestamp(time_unit, timezone) => {
                if let Some(timezone) = timezone {
                    write!(f, "Timestamp({time_unit}, {timezone:?})")
                } else {
                    write!(f, "Timestamp({time_unit})")
                }
            }
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Duration(time_unit) => write!(f, "Duration({time_unit})"),
            Self::Binary => write!(f, "Binary"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::List(field) => {
                let name = field.name();
                let maybe_nullable = format_nullability(field);
                let data_type = field.data_type();

                let field_name_str = if name == "item" {
                    String::default()
                } else {
                    format!(", field: '{name}'")
                };

                let metadata_str = format_metadata(field.metadata());

                // e.g. `List(non-null Uint32)
                write!(
                    f,
                    "List({maybe_nullable}{data_type}{field_name_str}{metadata_str})"
                )
            }
            Self::Struct(fields) => {
                write!(f, "Struct(")?;
                if !fields.is_empty() {
                    let fields_str = fields
                        .iter()
                        .map(|field| format_field(field))
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, "{fields_str}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
        }
    }
}

/// Adapter to format a metadata HashMap consistently.
struct FormatMetadata<'a>(&'a HashMap<String, String>);

impl std::fmt::Display for FormatMetadata<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metadata = self.0;
        if metadata.is_empty() {
            Ok(())
        } else {
            let mut entries: Vec<(&String, &String)> = metadata.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            write!(f, ", metadata: ")?;
            f.debug_map().entries(entries).finish()
        }
    }
}
