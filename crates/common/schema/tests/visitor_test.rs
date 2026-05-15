//! Integration tests for SchemaVisitor functionality

use std::sync::Arc;

mod common;

use common::*;
use wings_schema::{
    DataType, Field, SchemaBuilder, SchemaError, SchemaVisitor,
    visitor::{visit_field, visit_schema},
};

// ============================================
// Test Visitor Implementations
// ============================================

/// Collects all field names visited
#[derive(Debug, Default)]
struct FieldNameCollector {
    names: Vec<String>,
}

impl SchemaVisitor for FieldNameCollector {
    type Error = SchemaError;

    fn field(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        self.names.push(field.name().to_string());
        Ok(())
    }

    fn before_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn after_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn before_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Tracks the order of visitor callbacks
#[derive(Debug, Default)]
struct CallbackTracker {
    events: Vec<String>,
}

impl SchemaVisitor for CallbackTracker {
    type Error = SchemaError;

    fn field(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        self.events.push(format!("field: {}", field.name()));
        Ok(())
    }

    fn before_list_element(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        self.events.push(format!("before_list: {}", field.name()));
        Ok(())
    }

    fn after_list_element(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        self.events.push(format!("after_list: {}", field.name()));
        Ok(())
    }

    fn before_struct_field(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        self.events.push(format!("before_struct: {}", field.name()));
        Ok(())
    }

    fn after_struct_field(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        self.events.push(format!("after_struct: {}", field.name()));
        Ok(())
    }
}

/// Visitor that fails on specific field names
struct FailingVisitor {
    fail_on: String,
}

impl SchemaVisitor for FailingVisitor {
    type Error = SchemaError;

    fn field(&mut self, field: &Arc<Field>) -> Result<(), Self::Error> {
        if field.name() == self.fail_on {
            return Err(SchemaError::UnsupportedDataType {
                data_type: field.data_type().clone(),
            });
        }
        Ok(())
    }

    fn before_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn after_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn before_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Counts the depth of nested structures
#[derive(Debug, Default)]
struct DepthCounter {
    current_depth: usize,
    max_depth: usize,
}

impl SchemaVisitor for DepthCounter {
    type Error = SchemaError;

    fn field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn before_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.current_depth += 1;
        self.max_depth = self.max_depth.max(self.current_depth);
        Ok(())
    }

    fn after_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.current_depth -= 1;
        Ok(())
    }

    fn before_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.current_depth += 1;
        self.max_depth = self.max_depth.max(self.current_depth);
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.current_depth -= 1;
        Ok(())
    }
}

// ============================================
// Basic Visitor Tests
// ============================================

#[test]
fn test_visit_schema_simple() {
    let fields = vec![
        test_field("a", 1, DataType::Utf8, false),
        test_field("b", 2, DataType::Int32, true),
    ];
    let schema = SchemaBuilder::new(fields).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    assert_eq!(collector.names, vec!["a", "b"]);
}

#[test]
fn test_visit_field_simple() {
    let field = Arc::new(test_field("simple", 1, DataType::Int32, true));

    let mut collector = FieldNameCollector::default();
    visit_field(&field, &mut collector).unwrap();

    assert_eq!(collector.names, vec!["simple"]);
}

// ============================================
// List Tests
// ============================================

#[test]
fn test_visit_field_list() {
    let field = Arc::new(list_field("tags", 1, DataType::Utf8));

    let mut collector = FieldNameCollector::default();
    visit_field(&field, &mut collector).unwrap();

    // Should visit: list field (after item) and item field
    assert_eq!(collector.names.len(), 2);
    assert!(collector.names.contains(&"tags".to_string()));
    assert!(collector.names.contains(&"item".to_string()));
}

#[test]
fn test_visit_field_list_callback_order() {
    let field = Arc::new(list_field("items", 1, DataType::Int32));

    let mut tracker = CallbackTracker::default();
    visit_field(&field, &mut tracker).unwrap();

    // Verify callback order (post-order traversal)
    // 1. before_list_element for "items"
    // 2. field for "item"
    // 3. after_list_element for "items"
    // 4. field for "items"
    assert!(tracker.events.iter().any(|e| e == "before_list: items"));
    assert!(tracker.events.iter().any(|e| e == "field: item"));
    assert!(tracker.events.iter().any(|e| e == "after_list: items"));
    assert!(tracker.events.iter().any(|e| e == "field: items"));
}

#[test]
fn test_visit_schema_with_list() {
    let fields = vec![
        test_field("name", 1, DataType::Utf8, false),
        list_field("tags", 2, DataType::Utf8),
    ];
    let schema = SchemaBuilder::new(fields).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    assert_eq!(collector.names.len(), 3);
    assert!(collector.names.contains(&"name".to_string()));
    assert!(collector.names.contains(&"tags".to_string()));
    assert!(collector.names.contains(&"item".to_string()));
}

// ============================================
// Struct Tests
// ============================================

#[test]
fn test_visit_field_struct() {
    let nested = vec![
        test_field("x", 10, DataType::Int32, false),
        test_field("y", 11, DataType::Int32, false),
    ];
    let field = Arc::new(struct_field("point", 1, nested));

    let mut collector = FieldNameCollector::default();
    visit_field(&field, &mut collector).unwrap();

    // Should visit: point (after children), x, y
    assert_eq!(collector.names.len(), 3);
    assert!(collector.names.contains(&"point".to_string()));
    assert!(collector.names.contains(&"x".to_string()));
    assert!(collector.names.contains(&"y".to_string()));
}

#[test]
fn test_visit_field_struct_callback_order() {
    let nested = vec![test_field("street", 10, DataType::Utf8, false)];
    let field = Arc::new(struct_field("address", 1, nested));

    let mut tracker = CallbackTracker::default();
    visit_field(&field, &mut tracker).unwrap();

    // Verify callback order
    assert!(tracker.events.iter().any(|e| e == "before_struct: address"));
    assert!(tracker.events.iter().any(|e| e == "field: street"));
    assert!(tracker.events.iter().any(|e| e == "after_struct: address"));
    assert!(tracker.events.iter().any(|e| e == "field: address"));
}

#[test]
fn test_visit_schema_with_struct() {
    let address_nested = vec![
        test_field("street", 10, DataType::Utf8, false),
        test_field("city", 11, DataType::Utf8, false),
    ];
    let fields = vec![
        test_field("name", 1, DataType::Utf8, false),
        struct_field("address", 2, address_nested),
    ];
    let schema = SchemaBuilder::new(fields).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    assert_eq!(collector.names.len(), 4);
    assert!(collector.names.contains(&"name".to_string()));
    assert!(collector.names.contains(&"address".to_string()));
    assert!(collector.names.contains(&"street".to_string()));
    assert!(collector.names.contains(&"city".to_string()));
}

// ============================================
// Nested Structure Tests
// ============================================

#[test]
fn test_visit_deeply_nested_struct() {
    // Create: person -> address -> coordinates (point)
    let coordinates = vec![
        test_field("lat", 100, DataType::Float64, false),
        test_field("lng", 101, DataType::Float64, false),
    ];
    let address_fields = vec![
        test_field("street", 10, DataType::Utf8, false),
        struct_field("coordinates", 11, coordinates),
    ];
    let person_fields = vec![
        test_field("name", 1, DataType::Utf8, false),
        struct_field("address", 2, address_fields),
    ];

    let schema = SchemaBuilder::new(person_fields).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    assert_eq!(collector.names.len(), 6);
    assert!(collector.names.contains(&"name".to_string()));
    assert!(collector.names.contains(&"address".to_string()));
    assert!(collector.names.contains(&"street".to_string()));
    assert!(collector.names.contains(&"coordinates".to_string()));
    assert!(collector.names.contains(&"lat".to_string()));
    assert!(collector.names.contains(&"lng".to_string()));
}

#[test]
fn test_visit_list_in_struct() {
    let person_fields = vec![
        test_field("name", 1, DataType::Utf8, false),
        list_field("emails", 2, DataType::Utf8),
    ];

    let schema = SchemaBuilder::new(person_fields).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    assert_eq!(collector.names.len(), 3);
    assert!(collector.names.contains(&"name".to_string()));
    assert!(collector.names.contains(&"emails".to_string()));
    assert!(collector.names.contains(&"item".to_string()));
}

#[test]
fn test_visit_nested_lists() {
    // List of lists: items is List(List(Int32))
    let inner_item = Arc::new(Field::new("item", 100, DataType::Int32, true));
    let middle_item = Arc::new(Field::new("item", 10, DataType::List(inner_item), true));
    let list_field = Field::new("matrix", 1, DataType::List(middle_item), true);

    let schema = SchemaBuilder::new(vec![list_field]).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    // Should visit: matrix, item (middle), item (inner)
    assert_eq!(collector.names.len(), 3);
    assert!(collector.names.contains(&"matrix".to_string()));
}

#[test]
fn test_depth_counter() {
    // Create deeply nested structure
    let level3 = vec![test_field("deep", 100, DataType::Int32, false)];
    let level2 = vec![struct_field("level2", 10, level3)];
    let level1 = vec![struct_field("level1", 2, level2)];
    let root = vec![struct_field("root", 1, level1)];

    let schema = SchemaBuilder::new(root).build().unwrap();

    let mut counter = DepthCounter::default();
    visit_schema(&schema, &mut counter).unwrap();

    assert_eq!(counter.max_depth, 3);
}

// ============================================
// Error Handling Tests
// ============================================

#[test]
fn test_visit_schema_error_propagation() {
    let fields = vec![
        test_field("good", 1, DataType::Utf8, false),
        test_field("bad", 2, DataType::Int32, true),
    ];
    let schema = SchemaBuilder::new(fields).build().unwrap();

    let mut visitor = FailingVisitor {
        fail_on: "bad".to_string(),
    };
    let result = visit_schema(&schema, &mut visitor);

    assert!(result.is_err());
}

#[test]
fn test_visit_field_error_in_nested() {
    let nested = vec![test_field("bad", 10, DataType::Int32, false)];
    let field = Arc::new(struct_field("container", 1, nested));

    let mut visitor = FailingVisitor {
        fail_on: "bad".to_string(),
    };
    let result = visit_field(&field, &mut visitor);

    assert!(result.is_err());
}

// ============================================
// Empty Schema Tests
// ============================================

#[test]
fn test_visit_empty_schema() {
    let schema = SchemaBuilder::new(vec![] as Vec<Field>).build().unwrap();

    let mut collector = FieldNameCollector::default();
    visit_schema(&schema, &mut collector).unwrap();

    assert!(collector.names.is_empty());
}

#[test]
fn test_visit_empty_struct() {
    let field = Arc::new(struct_field("empty_struct", 1, vec![]));

    let mut collector = FieldNameCollector::default();
    visit_field(&field, &mut collector).unwrap();

    // Should only visit the struct field itself (no children)
    assert_eq!(collector.names, vec!["empty_struct"]);
}
