use std::sync::Arc;

use datafusion::{
    common::arrow::datatypes::{DataType, Field, Schema},
    physical_plan::{ExecutionPlan, displayable, empty::EmptyExec},
};
use wings_server_core::datafusion_helpers::apply_projection;

fn create_test_plan() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    Arc::new(EmptyExec::new(schema))
}

#[test]
fn test_apply_projection_with_some() {
    let exec_plan = create_test_plan();

    // Project only columns 0 and 2 (id and age)
    let projection = vec![0, 2];
    let result = apply_projection(exec_plan, Some(&projection)).expect("should apply projection");
    let result = displayable(result.as_ref());

    insta::assert_snapshot!(result.indent(true), @r"
    ProjectionExec: expr=[id@0 as id, age@2 as age]
      EmptyExec
    ");
}

#[test]
fn test_apply_projection_with_none() {
    let exec_plan = create_test_plan();

    // No projection
    let result = apply_projection(exec_plan.clone(), None).expect("should return same plan");
    let result = displayable(result.as_ref());

    insta::assert_snapshot!(result.indent(true), @"EmptyExec");
}

#[test]
fn test_apply_projection_single_column() {
    let exec_plan = create_test_plan();

    // Project only column 1 (name)
    let projection = vec![1];
    let result = apply_projection(exec_plan, Some(&projection)).expect("should apply projection");
    let result = displayable(result.as_ref());

    insta::assert_snapshot!(result.indent(true), @r"
    ProjectionExec: expr=[name@1 as name]
      EmptyExec
    ");
}

#[test]
fn test_apply_projection_all_columns() {
    let exec_plan = create_test_plan();

    // Project all columns in order
    let projection = vec![0, 1, 2];
    let result = apply_projection(exec_plan, Some(&projection)).expect("should apply projection");
    let result = displayable(result.as_ref());

    insta::assert_snapshot!(result.indent(true), @r"
    ProjectionExec: expr=[id@0 as id, name@1 as name, age@2 as age]
      EmptyExec
    ");
}

#[test]
fn test_apply_projection_reordered() {
    let exec_plan = create_test_plan();

    // Project columns in reverse order
    let projection = vec![2, 1, 0];
    let result = apply_projection(exec_plan, Some(&projection)).expect("should apply projection");
    let result = displayable(result.as_ref());

    insta::assert_snapshot!(result.indent(true), @r"
    ProjectionExec: expr=[age@2 as age, name@1 as name, id@0 as id]
      EmptyExec
    ");
}
