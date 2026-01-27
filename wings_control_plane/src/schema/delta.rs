//! Convert wings schema to delta schema

use crate::schema::{DataType, Field, SchemaError, TimeUnit};

impl TryFrom<&Field> for deltalake_core::StructField {
    type Error = SchemaError;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        let data_type: deltalake_core::DataType = field.data_type().try_into()?;
        Ok(Self::new(field.name(), data_type, field.is_nullable()))
    }
}

impl TryFrom<Field> for deltalake_core::StructField {
    type Error = SchemaError;

    fn try_from(field: Field) -> Result<Self, Self::Error> {
        (&field).try_into()
    }
}

// Implementation based on the delta-io/delta-kernel-rs crate.
// https://github.com/delta-io/delta-kernel-rs/blob/70719a52ba19ccbb7bdab1d184046329597de9ab/kernel/src/engine/arrow_conversion.rs
impl TryFrom<&DataType> for deltalake_core::DataType {
    type Error = SchemaError;

    fn try_from(dt: &DataType) -> Result<Self, Self::Error> {
        use deltalake_core::{
            ArrayType as AT, DataType as DT, PrimitiveType as PT, StructField as SF,
            StructType as ST,
        };

        let out = match dt {
            // Null is not supported
            DataType::Boolean => DT::Primitive(PT::Boolean),
            DataType::Int8 => DT::Primitive(PT::Byte),
            DataType::Int16 => DT::Primitive(PT::Short),
            DataType::Int32 => DT::Primitive(PT::Integer),
            DataType::Int64 => DT::Primitive(PT::Long),
            // Unsigned integers are converted to signed integers?
            DataType::UInt8 => DT::Primitive(PT::Byte),
            DataType::UInt16 => DT::Primitive(PT::Short),
            DataType::UInt32 => DT::Primitive(PT::Integer),
            DataType::UInt64 => DT::Primitive(PT::Long),
            // Timestamp with arbitrary timezone is not supported
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                DT::Primitive(PT::Timestamp)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                DT::Primitive(PT::Timestamp)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => DT::Primitive(PT::TimestampNtz),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => DT::Primitive(PT::TimestampNtz),
            DataType::Date32 => DT::Primitive(PT::Date),
            DataType::Date64 => DT::Primitive(PT::Date),
            // Duration is not supported
            DataType::Binary => DT::Primitive(PT::Binary),
            DataType::Utf8 => DT::Primitive(PT::String),
            DataType::List(field) => {
                AT::new(field.data_type().try_into()?, field.is_nullable()).into()
            }
            DataType::Struct(fields) => {
                let delta_fields = fields
                    .iter()
                    .map(|f| {
                        let dt: DT = f.data_type().try_into()?;
                        Ok(SF::new(f.name(), dt, f.is_nullable()))
                    })
                    .collect::<Result<Vec<_>, SchemaError>>()?;
                // SAFETY: field names are unique
                ST::new_unchecked(delta_fields).into()
            }
            _ => {
                return Err(SchemaError::UnsupportedDataType {
                    data_type: dt.clone(),
                });
            }
        };

        Ok(out)
    }
}

impl TryFrom<DataType> for deltalake_core::DataType {
    type Error = SchemaError;

    fn try_from(dt: DataType) -> Result<Self, Self::Error> {
        (&dt).try_into()
    }
}
