use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    physical_plan::{ExecutionPlan, PhysicalExpr, expressions::col, projection::ProjectionExec},
};

pub fn apply_projection(
    exec_plan: Arc<dyn ExecutionPlan>,
    projection: Option<&Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let Some(projection) = projection else {
        return Ok(exec_plan);
    };

    let base_schema = exec_plan.schema();
    let projected_exprs: Vec<(Arc<dyn PhysicalExpr + 'static>, String)> = projection
        .iter()
        .map(|&i| {
            let field_name = base_schema.field(i).name();
            let col_expr = col(field_name, &base_schema)?;
            Ok::<_, DataFusionError>((col_expr, field_name.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let projection_exec = ProjectionExec::try_new(projected_exprs, exec_plan)?;

    Ok(Arc::new(projection_exec))
}
