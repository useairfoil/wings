use crate::{DataType, FieldRef, Schema};

pub trait SchemaVisitor {
    type Error;

    fn field(&mut self, field: &FieldRef) -> Result<(), Self::Error>;
    fn before_list_element(&mut self, field: &FieldRef) -> Result<(), Self::Error>;
    fn after_list_element(&mut self, field: &FieldRef) -> Result<(), Self::Error>;
    fn before_struct_field(&mut self, field: &FieldRef) -> Result<(), Self::Error>;
    fn after_struct_field(&mut self, field: &FieldRef) -> Result<(), Self::Error>;
}

pub fn visit_field<V: SchemaVisitor>(
    root_field: &FieldRef,
    visitor: &mut V,
) -> Result<(), V::Error> {
    match root_field.data_type() {
        DataType::List(field) => {
            visitor.before_list_element(root_field)?;
            visit_field(field, visitor)?;
            visitor.after_list_element(root_field)?;
        }
        DataType::Struct(fields) => {
            for field in fields.iter() {
                visitor.before_struct_field(root_field)?;
                visit_field(field, visitor)?;
                visitor.after_struct_field(root_field)?;
            }
        }
        _ => {}
    }

    visitor.field(root_field)
}

/// Visit schema in post order.
pub fn visit_schema<V: SchemaVisitor>(schema: &Schema, visitor: &mut V) -> Result<(), V::Error> {
    for field in schema.fields.iter() {
        visit_field(field, visitor)?;
    }

    Ok(())
}
