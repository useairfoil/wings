use std::collections::HashMap;

use snafu::Snafu;
use wings_schema::{FieldRef, Schema, SchemaVisitor, visit_schema};

#[derive(Debug, Snafu)]
#[snafu(display(
    "Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}"
))]
pub struct DuplicateFieldNameError {
    pub full_name: String,
    pub field_id: u64,
    pub existing_field_id: u64,
}

pub fn create_parquet_path_index(
    schema: &Schema,
) -> Result<IndexByParquetPathName, DuplicateFieldNameError> {
    let mut visitor = IndexByParquetPathName::new();
    visit_schema(schema, &mut visitor)?;
    Ok(visitor)
}

/// A mapping from Parquet column path names to internal field id
pub struct IndexByParquetPathName {
    name_to_id: HashMap<String, u64>,
    field_names: Vec<String>,
}

impl IndexByParquetPathName {
    /// Creates a new, empty `IndexByParquetPathName`
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            field_names: Vec::new(),
        }
    }

    /// Retrieves the internal field ID
    pub fn get(&self, name: &str) -> Option<&u64> {
        self.name_to_id.get(name)
    }
}

impl Default for IndexByParquetPathName {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type Error = DuplicateFieldNameError;

    fn before_struct_field(&mut self, field: &FieldRef) -> Result<(), Self::Error> {
        self.field_names.push(field.name.to_string());
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &FieldRef) -> Result<(), Self::Error> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &FieldRef) -> Result<(), Self::Error> {
        self.field_names.push(format!("{}.list", field.name));
        Ok(())
    }

    fn after_list_element(&mut self, _field: &FieldRef) -> Result<(), Self::Error> {
        self.field_names.pop();
        Ok(())
    }

    fn field(&mut self, field: &FieldRef) -> Result<(), Self::Error> {
        let full_name = if self.field_names.is_empty() {
            field.name().to_string()
        } else {
            let parent_name = self.field_names.join(".");
            format!("{parent_name}.{}", field.name())
        };

        let field_id = field.id as _;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(DuplicateFieldNameError {
                full_name,
                field_id,
                existing_field_id: *existing_field_id,
            });
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use wings_schema::{DataType, Field, SchemaBuilder};

    use super::create_parquet_path_index;

    #[test]
    pub fn test_nested_schema() {
        let schema = SchemaBuilder::new(vec![
            Field::new("zero", 0, DataType::Binary, true),
            Field::new("one", 1, DataType::Binary, true),
            Field::new(
                "two",
                2,
                DataType::Struct(
                    vec![
                        Field::new("a", 20, DataType::Date64, false),
                        Field::new("b", 21, DataType::Date64, false),
                        Field::new(
                            "c",
                            22,
                            DataType::Struct(
                                vec![Field::new("z", 220, DataType::Binary, true)].into(),
                            ),
                            false,
                        ),
                        Field::new(
                            "d",
                            23,
                            DataType::List(Field::new("z", 230, DataType::Binary, false).into()),
                            false,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "three",
                3,
                DataType::List(Field::new("element", 30, DataType::Binary, true).into()),
                true,
            ),
        ])
        .build()
        .unwrap();

        let index = create_parquet_path_index(&schema).unwrap();

        let expected = HashMap::from([
            ("zero".to_string(), 0),
            ("one".to_string(), 1),
            ("two".to_string(), 2),
            ("two.a".to_string(), 20),
            ("two.b".to_string(), 21),
            ("two.c".to_string(), 22),
            ("two.c.z".to_string(), 220),
            ("two.d".to_string(), 23),
            ("two.d.list.z".to_string(), 230),
            ("three".to_string(), 3),
            ("three.list.element".to_string(), 30),
        ]);

        for (path, id) in expected.iter() {
            assert_eq!(index.get(path), Some(id), "field: {}", path);
        }

        assert_eq!(expected.len(), index.name_to_id.len());
    }
}
