use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, DurationMicrosecondArray,
        DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
        StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    buffer::OffsetBuffer,
};
use parquet::file::properties::WriterProperties;
use wings_data_lake::ParquetWriter;
use wings_schema::{DataType, Datum, Field, SchemaBuilder, SchemaRef, TimeUnit};

fn create_test_schema() -> SchemaRef {
    let schema = SchemaBuilder::new(vec![
        Field::new("id", 2, DataType::Int32, false),
        Field::new("name", 3, DataType::Utf8, false),
    ])
    .build()
    .unwrap();

    Arc::new(schema)
}

fn create_test_batch() -> RecordBatch {
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);

    RecordBatch::try_new(
        create_test_schema().arrow_schema().into(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .unwrap()
}

/// Creates a comprehensive schema with all data types including struct, list, and timestamp variants
fn create_comprehensive_schema() -> SchemaRef {
    // Define struct fields for address
    let address_fields: Vec<Field> = vec![
        Field::new("street", 100, DataType::Utf8, true),
        Field::new("city", 101, DataType::Utf8, true),
        Field::new("zip_code", 102, DataType::Utf8, true),
    ];

    // Define struct fields for list items
    let item_fields: Vec<Field> = vec![
        Field::new("product_id", 200, DataType::Int32, true),
        Field::new("quantity", 201, DataType::Int32, true),
        Field::new("price", 202, DataType::Float64, true),
    ];

    let schema = SchemaBuilder::new(vec![
        // Primitive types
        Field::new("null_col", 1, DataType::Null, true),
        Field::new("bool_col", 2, DataType::Boolean, true),
        Field::new("int8_col", 3, DataType::Int8, true),
        Field::new("int16_col", 4, DataType::Int16, true),
        Field::new("int32_col", 5, DataType::Int32, true),
        Field::new("int64_col", 6, DataType::Int64, true),
        Field::new("uint8_col", 7, DataType::UInt8, true),
        Field::new("uint16_col", 8, DataType::UInt16, true),
        Field::new("uint32_col", 9, DataType::UInt32, true),
        Field::new("uint64_col", 10, DataType::UInt64, true),
        Field::new("float32_col", 11, DataType::Float32, true),
        Field::new("float64_col", 12, DataType::Float64, true),
        Field::new("string_col", 13, DataType::Utf8, true),
        Field::new("binary_col", 14, DataType::Binary, true),
        // Date/Time types
        Field::new("date32_col", 15, DataType::Date32, true),
        Field::new("date64_col", 16, DataType::Date64, true),
        // Timestamp types with different time units
        Field::new(
            "timestamp_sec",
            17,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "timestamp_ms",
            19,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "timestamp_us",
            20,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_ns",
            21,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        // Duration types with different time units
        Field::new(
            "duration_sec",
            22,
            DataType::Duration(TimeUnit::Second),
            true,
        ),
        Field::new(
            "duration_ms",
            23,
            DataType::Duration(TimeUnit::Millisecond),
            true,
        ),
        Field::new(
            "duration_us",
            24,
            DataType::Duration(TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "duration_ns",
            25,
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        // Complex types
        Field::new("address", 26, DataType::Struct(address_fields.into()), true),
        Field::new(
            "items",
            27,
            DataType::List(Arc::new(
                Field::new("item", 28, DataType::Struct(item_fields.into()), true).into(),
            )),
            true,
        ),
    ])
    .build()
    .unwrap();

    Arc::new(schema)
}

/// Creates a test batch with 3 rows, no nulls for the comprehensive schema
fn create_comprehensive_batch_no_nulls() -> RecordBatch {
    let schema = create_comprehensive_schema();
    let arrow_schema = schema.arrow_schema();

    // Primitive arrays
    let null_array = arrow::array::NullArray::new(3);
    let bool_array = BooleanArray::from(vec![true, false, true]);
    let int8_array = Int8Array::from(vec![1i8, 2i8, 3i8]);
    let int16_array = Int16Array::from(vec![10i16, 20i16, 30i16]);
    let int32_array = Int32Array::from(vec![100i32, 200i32, 300i32]);
    let int64_array = Int64Array::from(vec![1000i64, 2000i64, 3000i64]);
    let uint8_array = UInt8Array::from(vec![1u8, 2u8, 3u8]);
    let uint16_array = UInt16Array::from(vec![10u16, 20u16, 30u16]);
    let uint32_array = UInt32Array::from(vec![100u32, 200u32, 300u32]);
    let uint64_array = UInt64Array::from(vec![1000u64, 2000u64, 3000u64]);
    let float32_array = Float32Array::from(vec![1.1f32, 2.2f32, 3.3f32]);
    let float64_array = Float64Array::from(vec![10.1f64, 20.2f64, 30.3f64]);
    let string_array = StringArray::from(vec!["alice", "bob", "charlie"]);
    let binary_array = arrow::array::BinaryArray::from(vec![
        b"binary1".as_ref(),
        b"binary2".as_ref(),
        b"binary3".as_ref(),
    ]);

    // Date/Time arrays
    let date32_array = Date32Array::from(vec![1, 2, 3]); // days since epoch
    let date64_array = Date64Array::from(vec![86400000, 172800000, 259200000]); // milliseconds
    let timestamp_sec_array = TimestampSecondArray::from(vec![1, 2, 3]);
    let timestamp_ms_array = TimestampMillisecondArray::from(vec![1000, 2000, 3000]);
    let timestamp_us_array = TimestampMicrosecondArray::from(vec![1000000, 2000000, 3000000]);
    let timestamp_ns_array =
        TimestampNanosecondArray::from(vec![1000000000, 2000000000, 3000000000]);
    let duration_sec_array = DurationSecondArray::from(vec![1, 2, 3]);
    let duration_ms_array = DurationMillisecondArray::from(vec![1000, 2000, 3000]);
    let duration_us_array = DurationMicrosecondArray::from(vec![1000000, 2000000, 3000000]);
    let duration_ns_array = DurationNanosecondArray::from(vec![1000000000, 2000000000, 3000000000]);

    // Complex arrays - Address struct
    let address_street = StringArray::from(vec!["123 Main St", "456 Oak Ave", "789 Pine Rd"]);
    let address_city = StringArray::from(vec!["Anytown", "Somewhere", "Elsewhere"]);
    let address_zip = StringArray::from(vec!["12345", "67890", "11111"]);

    let address_struct = StructArray::from(vec![
        (
            Field::new("street", 100, DataType::Utf8, true)
                .into_arrow_field()
                .into(),
            Arc::new(address_street) as ArrayRef,
        ),
        (
            Field::new("city", 101, DataType::Utf8, true)
                .into_arrow_field()
                .into(),
            Arc::new(address_city) as ArrayRef,
        ),
        (
            Field::new("zip_code", 102, DataType::Utf8, true)
                .into_arrow_field()
                .into(),
            Arc::new(address_zip) as ArrayRef,
        ),
    ]);

    // Complex arrays - List of structs
    let item_product_ids = Int32Array::from(vec![101, 102, 103, 201, 202, 203, 301, 302, 303]);
    let item_quantities = Int32Array::from(vec![1, 2, 1, 3, 1, 2, 1, 1, 2]);
    let item_prices = Float64Array::from(vec![
        10.50, 20.00, 15.75, 25.00, 30.00, 18.50, 22.25, 35.00, 28.00,
    ]);

    let item_struct = StructArray::from(vec![
        (
            Field::new("product_id", 200, DataType::Int32, true)
                .into_arrow_field()
                .into(),
            Arc::new(item_product_ids.clone()) as ArrayRef,
        ),
        (
            Field::new("quantity", 201, DataType::Int32, true)
                .into_arrow_field()
                .into(),
            Arc::new(item_quantities.clone()) as ArrayRef,
        ),
        (
            Field::new("price", 202, DataType::Float64, true)
                .into_arrow_field()
                .into(),
            Arc::new(item_prices.clone()) as ArrayRef,
        ),
    ]);

    // Create list array with offsets [0, 3, 6, 9] representing 3 items per row
    let list_offsets = OffsetBuffer::new(vec![0i32, 3, 6, 9].into());
    let item_fields: Vec<Field> = vec![
        Field::new("product_id", 200, DataType::Int32, true),
        Field::new("quantity", 201, DataType::Int32, true),
        Field::new("price", 202, DataType::Float64, true),
    ];
    let items_list_array = ListArray::new(
        Field::new("item", 28, DataType::Struct(item_fields.into()), true)
            .into_arrow_field()
            .into(),
        list_offsets,
        Arc::new(item_struct),
        None,
    );

    RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(null_array),
            Arc::new(bool_array),
            Arc::new(int8_array),
            Arc::new(int16_array),
            Arc::new(int32_array),
            Arc::new(int64_array),
            Arc::new(uint8_array),
            Arc::new(uint16_array),
            Arc::new(uint32_array),
            Arc::new(uint64_array),
            Arc::new(float32_array),
            Arc::new(float64_array),
            Arc::new(string_array),
            Arc::new(binary_array),
            Arc::new(date32_array),
            Arc::new(date64_array),
            Arc::new(timestamp_sec_array),
            Arc::new(timestamp_ms_array),
            Arc::new(timestamp_us_array),
            Arc::new(timestamp_ns_array),
            Arc::new(duration_sec_array),
            Arc::new(duration_ms_array),
            Arc::new(duration_us_array),
            Arc::new(duration_ns_array),
            Arc::new(address_struct),
            Arc::new(items_list_array),
        ],
    )
    .unwrap()
}

/// Creates a test batch with 2 rows, one with nulls for the comprehensive schema
fn create_comprehensive_batch_with_nulls() -> RecordBatch {
    let schema = create_comprehensive_schema();
    let arrow_schema = schema.arrow_schema();

    // Primitive arrays - row 1 has values, row 2 has nulls where nullable
    let null_array = arrow::array::NullArray::new(2);
    let bool_array = BooleanArray::from(vec![Some(true), None]);
    let int8_array = Int8Array::from(vec![Some(1i8), None]);
    let int16_array = Int16Array::from(vec![Some(10i16), None]);
    let int32_array = Int32Array::from(vec![Some(100i32), None]);
    let int64_array = Int64Array::from(vec![Some(1000i64), None]);
    let uint8_array = UInt8Array::from(vec![Some(1u8), None]);
    let uint16_array = UInt16Array::from(vec![Some(10u16), None]);
    let uint32_array = UInt32Array::from(vec![Some(100u32), None]);
    let uint64_array = UInt64Array::from(vec![Some(1000u64), None]);
    let float32_array = Float32Array::from(vec![Some(1.1f32), None]);
    let float64_array = Float64Array::from(vec![Some(10.1f64), None]);
    let string_array = StringArray::from(vec![Some("alice"), None]);
    let binary_array = arrow::array::BinaryArray::from(vec![Some(b"binary1".as_ref()), None]);

    // Date/Time arrays
    let date32_array = Date32Array::from(vec![Some(1), None]); // days since epoch
    let date64_array = Date64Array::from(vec![Some(86400000), None]); // milliseconds
    let duration_sec_array = DurationSecondArray::from(vec![Some(1), None]);
    let duration_ms_array = DurationMillisecondArray::from(vec![Some(1000), None]);
    let duration_us_array = DurationMicrosecondArray::from(vec![Some(1000000), None]);
    let duration_ns_array = DurationNanosecondArray::from(vec![Some(1000000000), None]);
    let timestamp_sec_array = TimestampSecondArray::from(vec![Some(1), None]);
    let timestamp_ms_array = TimestampMillisecondArray::from(vec![Some(1000), None]);
    let timestamp_us_array = TimestampMicrosecondArray::from(vec![Some(1000000), None]);
    let timestamp_ns_array = TimestampNanosecondArray::from(vec![Some(1000000000), None]);

    // Complex arrays - Address struct with null zip_code in row 2
    let address_street = StringArray::from(vec![Some("123 Main St"), None]);
    let address_city = StringArray::from(vec![Some("Anytown"), Some("Somewhere")]);
    let address_zip = StringArray::from(vec![Some("12345"), None]); // null in row 2

    let address_struct = StructArray::from(vec![
        (
            Field::new("street", 100, DataType::Utf8, true)
                .into_arrow_field()
                .into(),
            Arc::new(address_street) as ArrayRef,
        ),
        (
            Field::new("city", 101, DataType::Utf8, true)
                .into_arrow_field()
                .into(),
            Arc::new(address_city) as ArrayRef,
        ),
        (
            Field::new("zip_code", 102, DataType::Utf8, true)
                .into_arrow_field()
                .into(),
            Arc::new(address_zip) as ArrayRef,
        ),
    ]);

    // Complex arrays - List of structs with fewer items in row 2
    let item_product_ids = Int32Array::from(vec![101, 102, 103, 201]); // 3 items row 1, 1 item row 2
    let item_quantities = Int32Array::from(vec![1, 2, 1, 3]);
    let item_prices = Float64Array::from(vec![10.50, 20.00, 15.75, 25.00]);

    let item_struct = StructArray::from(vec![
        (
            Field::new("product_id", 200, DataType::Int32, true)
                .into_arrow_field()
                .into(),
            Arc::new(item_product_ids.clone()) as ArrayRef,
        ),
        (
            Field::new("quantity", 201, DataType::Int32, true)
                .into_arrow_field()
                .into(),
            Arc::new(item_quantities.clone()) as ArrayRef,
        ),
        (
            Field::new("price", 202, DataType::Float64, true)
                .into_arrow_field()
                .into(),
            Arc::new(item_prices.clone()) as ArrayRef,
        ),
    ]);

    // Create list array with offsets [0, 3, 4] representing 3 items row 1, 1 item row 2
    let list_offsets = OffsetBuffer::new(vec![0i32, 3, 4].into());
    let item_fields: Vec<Field> = vec![
        Field::new("product_id", 200, DataType::Int32, true),
        Field::new("quantity", 201, DataType::Int32, true),
        Field::new("price", 202, DataType::Float64, true),
    ];
    let items_list_array = ListArray::new(
        Field::new("item", 28, DataType::Struct(item_fields.into()), true)
            .into_arrow_field()
            .into(),
        list_offsets,
        Arc::new(item_struct),
        None,
    );

    RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(null_array),
            Arc::new(bool_array),
            Arc::new(int8_array),
            Arc::new(int16_array),
            Arc::new(int32_array),
            Arc::new(int64_array),
            Arc::new(uint8_array),
            Arc::new(uint16_array),
            Arc::new(uint32_array),
            Arc::new(uint64_array),
            Arc::new(float32_array),
            Arc::new(float64_array),
            Arc::new(string_array),
            Arc::new(binary_array),
            Arc::new(date32_array),
            Arc::new(date64_array),
            Arc::new(timestamp_sec_array),
            Arc::new(timestamp_ms_array),
            Arc::new(timestamp_us_array),
            Arc::new(timestamp_ns_array),
            Arc::new(duration_sec_array),
            Arc::new(duration_ms_array),
            Arc::new(duration_us_array),
            Arc::new(duration_ns_array),
            Arc::new(address_struct),
            Arc::new(items_list_array),
        ],
    )
    .unwrap()
}

#[test]
fn test_parquet_writer_write_and_flush() {
    let schema = create_test_schema();
    let writer_properties = WriterProperties::builder().build();
    let mut writer = ParquetWriter::new(schema, writer_properties);

    let batch = create_test_batch();
    writer.write(&batch).unwrap();

    let (data, metadata) = writer.finish().unwrap();

    assert!(!data.is_empty());
    assert_eq!(metadata.num_rows, 3);
    assert!(metadata.file_size.as_u64() > 0);
}

#[test]
fn test_current_file_size() {
    let schema = create_test_schema();
    let writer_properties = WriterProperties::builder().build();
    let mut writer = ParquetWriter::new(schema, writer_properties);

    let initial_size = writer.current_file_size();
    assert_eq!(initial_size, 0);

    let batch = create_test_batch();
    writer.write(&batch).unwrap();

    let size_after_write = writer.current_file_size();
    assert!(size_after_write > 0);
}

#[test]
fn test_flush_resets_writer() {
    let schema = create_test_schema();
    let writer_properties = WriterProperties::builder().build();
    let mut writer = ParquetWriter::new(schema, writer_properties);

    let batch = create_test_batch();
    writer.write(&batch).unwrap();

    let (data1, metadata1) = writer.finish().unwrap();
    assert!(!data1.is_empty());
    assert_eq!(metadata1.num_rows, 3);

    // Write another batch to the reset writer
    writer.write(&batch).unwrap();
    let (data2, metadata2) = writer.finish().unwrap();
    assert!(!data2.is_empty());
    assert_eq!(metadata2.num_rows, 3);
}

#[test]
fn test_comprehensive_parquet_writer_stats() {
    let schema = create_comprehensive_schema();
    let writer_properties = WriterProperties::builder().build();
    let mut writer = ParquetWriter::new(schema.clone(), writer_properties);

    let batch = create_comprehensive_batch_no_nulls();
    writer.write(&batch).unwrap();

    let (data, metadata) = writer.finish().unwrap();

    // Verify basic metadata
    assert!(!data.is_empty());
    assert_eq!(metadata.num_rows, 3);
    assert!(metadata.file_size.as_u64() > 0);

    // Define expected field statistics for leaf fields (only those that support statistics)
    let expected_field_stats = vec![
        // Primitive types
        (2, Datum::bool(false), Datum::bool(true)), // bool_col
        (3, Datum::i8(1i8), Datum::i8(3i8)),        // int8_col
        (4, Datum::i16(10i16), Datum::i16(30i16)),  // int16_col
        (5, Datum::i32(100), Datum::i32(300)),      // int32_col
        (6, Datum::i64(1000i64), Datum::i64(3000i64)), // int64_col
        (7, Datum::u8(1u8), Datum::u8(3u8)),        // uint8_col
        (8, Datum::u16(10u16), Datum::u16(30u16)),  // uint16_col
        (9, Datum::u32(100u32), Datum::u32(300u32)), // uint32_col
        (10, Datum::u64(1000u64), Datum::u64(3000u64)), // uint64_col
        (11, Datum::f32(1.1f32), Datum::f32(3.3f32)), // float32_col
        (12, Datum::f64(10.1f64), Datum::f64(30.3f64)), // float64_col
        (13, Datum::utf8("alice"), Datum::utf8("charlie")), // string_col
        (
            14,
            Datum::binary(b"binary1".to_vec()),
            Datum::binary(b"binary3".to_vec()),
        ), // binary_col
        (15, Datum::date32(1), Datum::date32(3)),   // date32_col
        (16, Datum::date64(86400000i64), Datum::date64(259200000i64)), // date64_col
        // Timestamp types
        (
            17,
            Datum::timestamp(TimeUnit::Second, 1i64),
            Datum::timestamp(TimeUnit::Second, 3i64),
        ), // timestamp_sec
        (
            19,
            Datum::timestamp(TimeUnit::Millisecond, 1000i64),
            Datum::timestamp(TimeUnit::Millisecond, 3000i64),
        ), // timestamp_ms
        (
            20,
            Datum::timestamp(TimeUnit::Microsecond, 1000000i64),
            Datum::timestamp(TimeUnit::Microsecond, 3000000i64),
        ), // timestamp_us
        (
            21,
            Datum::timestamp(TimeUnit::Nanosecond, 1000000000i64),
            Datum::timestamp(TimeUnit::Nanosecond, 3000000000i64),
        ), // timestamp_ns
        // Duration types
        (
            22,
            Datum::duration(TimeUnit::Second, 1i64),
            Datum::duration(TimeUnit::Second, 3i64),
        ), // duration_sec
        (
            23,
            Datum::duration(TimeUnit::Millisecond, 1000i64),
            Datum::duration(TimeUnit::Millisecond, 3000i64),
        ), // duration_ms
        (
            24,
            Datum::duration(TimeUnit::Microsecond, 1000000i64),
            Datum::duration(TimeUnit::Microsecond, 3000000i64),
        ), // duration_us
        (
            25,
            Datum::duration(TimeUnit::Nanosecond, 1000000000i64),
            Datum::duration(TimeUnit::Nanosecond, 3000000000i64),
        ), // duration_ns
        // Struct fields
        (100, Datum::utf8("123 Main St"), Datum::utf8("789 Pine Rd")), // address.street
        (101, Datum::utf8("Anytown"), Datum::utf8("Somewhere")),       // address.city
        (102, Datum::utf8("11111"), Datum::utf8("67890")),             // address.zip_code
        // List of struct fields
        (200, Datum::i32(101), Datum::i32(303)), // items.item.product_id
        (201, Datum::i32(1), Datum::i32(3)),     // items.item.quantity
        (202, Datum::f64(10.5), Datum::f64(35.0)), // items.item.price
    ];

    // Verify column statistics are populated
    assert!(!metadata.column_sizes.is_empty());
    assert!(!metadata.value_counts.is_empty());
    assert!(!metadata.null_value_counts.is_empty());

    // Assert all expected field statistics
    for (field_id, expected_lower, expected_upper) in expected_field_stats {
        // Check value count is 3 for all leaf fields (except lists)
        let expected_count = if field_id >= 200 { 9 } else { 3 };
        assert_eq!(
            metadata.value_counts.get(&field_id),
            Some(&expected_count),
            "Field {} should have value count of {}",
            field_id,
            expected_count
        );

        // Check null value count is 0 for all leaf fields
        assert_eq!(
            metadata.null_value_counts.get(&field_id),
            Some(&0),
            "Field {} should have null value count of 0",
            field_id
        );

        // Check lower bounds
        assert_eq!(
            metadata.lower_bounds.get(&field_id),
            Some(&expected_lower),
            "Field {} lower bound mismatch",
            field_id
        );

        // Check upper bounds
        assert_eq!(
            metadata.upper_bounds.get(&field_id),
            Some(&expected_upper),
            "Field {} upper bound mismatch",
            field_id
        );
    }
}

#[test]
fn test_comprehensive_writer_with_nulls() {
    let schema = create_comprehensive_schema();
    let writer_properties = WriterProperties::builder().build();
    let mut writer = ParquetWriter::new(schema, writer_properties);

    let batch = create_comprehensive_batch_with_nulls();
    writer.write(&batch).unwrap();

    let (data, metadata) = writer.finish().unwrap();

    assert!(!data.is_empty());
    assert_eq!(metadata.num_rows, 2);

    // Define expected field statistics for leaf fields with nulls (1 null each for 2 rows)
    let expected_field_ids_with_nulls = vec![
        // Primitive types (all nullable and have 1 null in row 2)
        2,  // bool_col
        3,  // int8_col
        4,  // int16_col
        5,  // int32_col
        6,  // int64_col
        7,  // uint8_col
        8,  // uint16_col
        9,  // uint32_col
        10, // uint64_col
        11, // float32_col
        12, // float64_col
        13, // string_col
        14, // binary_col
        15, // date32_col
        16, // date64_col
        // Timestamp types (all nullable and have 1 null in row 2)
        17, // timestamp_sec
        19, // timestamp_ms
        20, // timestamp_us
        21, // timestamp_ns
        // Duration types
        22, // duration_sec
        23, // duration_ms
        24, // duration_us
        25, // duration_ns
    ];

    // Verify column statistics are populated
    assert!(!metadata.column_sizes.is_empty());
    assert!(!metadata.value_counts.is_empty());
    assert!(!metadata.null_value_counts.is_empty());

    // Assert all expected field statistics
    for field_id in expected_field_ids_with_nulls {
        // Check value count is 2 for all leaf fields (2 rows total)
        assert_eq!(
            metadata.value_counts.get(&field_id),
            Some(&2),
            "Field {} should have value count of 2",
            field_id
        );

        // Check null value count - most fields have 1 null (row 2 has nulls)
        // but some fields like address.city have 0 nulls
        let expected_null_count = match field_id {
            101 => 0, // address.city has no nulls
            _ => 1,   // all other fields have 1 null in row 2
        };

        assert_eq!(
            metadata.null_value_counts.get(&field_id),
            Some(&expected_null_count),
            "Field {} should have {} null values",
            field_id,
            expected_null_count
        );
    }
}
