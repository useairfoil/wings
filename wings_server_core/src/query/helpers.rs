use std::ops::{Deref, RangeInclusive};

use datafusion::{
    error::DataFusionError,
    logical_expr::{BinaryExpr, Operator},
    prelude::{Expr, col},
    scalar::ScalarValue,
};
use wings_control_plane::admin::OFFSET_COLUMN_NAME;

pub type OffsetRangeInclusive = RangeInclusive<u64>;

enum OffsetPart {
    Eq(u64),
    Lower(u64),
    Upper(u64),
}

pub fn find_partition_column_value(
    column_name: &str,
    filters: &[Expr],
) -> Result<ScalarValue, DataFusionError> {
    match filters
        .iter()
        .flat_map(|expr| find_partition_column_value_in_expr(column_name, expr))
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => Err(DataFusionError::Plan(format!(
            "No {0} filter provided. You must provide a value for the partition column.",
            column_name
        ))),
        [value] => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "Multiple values found for partition column '{0}'.",
            column_name
        ))),
    }
}

pub fn validate_offset_filters(filters: &[Expr]) -> Result<OffsetRangeInclusive, DataFusionError> {
    match filters
        .iter()
        .flat_map(find_offset_bound_in_filter)
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => Err(DataFusionError::Plan(format!(
            "No {0} filter provided. You must provide a lower and upper bound for {0}.",
            OFFSET_COLUMN_NAME
        ))),
        [bound] => match bound {
            OffsetPart::Eq(offset) => Ok(*offset..=*offset),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid {0} filter provided. You must provide a lower and upper bound for {0}.",
                OFFSET_COLUMN_NAME
            ))),
        },
        [bound0, bound1] => match (bound0, bound1) {
            (OffsetPart::Lower(l), OffsetPart::Upper(u)) => Ok(*l..=*u),
            (OffsetPart::Upper(u), OffsetPart::Lower(l)) => Ok(*l..=*u),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid {0} filter provided. You must provide a lower and upper bound for {0}.",
                OFFSET_COLUMN_NAME
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "Invalid {0} filter provided. You must provide a lower and upper bound for {0}.",
            OFFSET_COLUMN_NAME
        ))),
    }
}

fn find_offset_bound_in_filter(filter: &Expr) -> Option<OffsetPart> {
    match filter {
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Gt,
        }) => offset_value(left, right).map(|offset| OffsetPart::Lower(offset + 1)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::GtEq,
        }) => offset_value(left, right).map(|offset| OffsetPart::Lower(offset)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Lt,
        }) => {
            let offset = offset_value(left, right)?;
            if offset == 0 {
                None
            } else {
                Some(OffsetPart::Upper(offset - 1))
            }
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::LtEq,
        }) => offset_value(left, right).map(|offset| OffsetPart::Upper(offset)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) => offset_value(left, right).map(|offset| OffsetPart::Eq(offset)),
        _ => None,
    }
}

fn offset_value(left: &Box<Expr>, right: &Box<Expr>) -> Option<u64> {
    if left.deref() != &col(OFFSET_COLUMN_NAME) {
        return None;
    }

    match right.deref() {
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v),
        _ => None,
    }
}

fn find_partition_column_value_in_expr(column_name: &str, filter: &Expr) -> Option<ScalarValue> {
    match filter {
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) => {
            if left.deref() != &col(column_name) {
                return None;
            }

            match right.deref() {
                Expr::Literal(s, _) => Some(s.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}
