use crate::schema::{DataType, TimeUnit};

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Datum {
    Null,
    Boolean(bool),
    UInt8(u8),
    Int8(i8),
    UInt16(u16),
    Int16(i16),
    UInt32(u32),
    Int32(i32),
    UInt64(u64),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    Date64(i64),
    Timestamp(TimeUnit, i64),
}

impl Datum {
    pub fn bool(val: bool) -> Self {
        Self::Boolean(val)
    }

    pub fn u8(val: u8) -> Self {
        Self::UInt8(val)
    }

    pub fn i8(val: i8) -> Self {
        Self::Int8(val)
    }

    pub fn u16(val: u16) -> Self {
        Self::UInt16(val)
    }

    pub fn i16(val: i16) -> Self {
        Self::Int16(val)
    }

    pub fn u32(val: u32) -> Self {
        Self::UInt32(val)
    }

    pub fn i32(val: i32) -> Self {
        Self::Int32(val)
    }

    pub fn u64(val: u64) -> Self {
        Self::UInt64(val)
    }

    pub fn i64(val: i64) -> Self {
        Self::Int64(val)
    }

    pub fn f32(val: f32) -> Self {
        Self::Float32(val)
    }

    pub fn f64(val: f64) -> Self {
        Self::Float64(val)
    }

    pub fn utf8(val: impl Into<String>) -> Self {
        Self::Utf8(val.into())
    }

    pub fn binary(val: Vec<u8>) -> Self {
        Self::Binary(val)
    }

    pub fn date32(val: i32) -> Self {
        Self::Date32(val)
    }

    pub fn date64(val: i64) -> Self {
        Self::Date64(val)
    }

    pub fn timestamp(unit: TimeUnit, val: i64) -> Self {
        Self::Timestamp(unit, val)
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::UInt8(_) => DataType::UInt8,
            Self::Int8(_) => DataType::Int8,
            Self::UInt16(_) => DataType::UInt16,
            Self::Int16(_) => DataType::Int16,
            Self::UInt32(_) => DataType::UInt32,
            Self::Int32(_) => DataType::Int32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Binary(_) => DataType::Binary,
            Self::Date32(_) => DataType::Date32,
            Self::Date64(_) => DataType::Date64,
            // TODO: should we drop the timezone or not?
            Self::Timestamp(unit, _) => DataType::Timestamp(*unit, None),
        }
    }
}
