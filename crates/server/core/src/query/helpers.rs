use std::ops::{Deref, RangeInclusive};

use datafusion::{
    error::DataFusionError,
    logical_expr::{BinaryExpr, Operator},
    prelude::{Expr, col},
    scalar::ScalarValue,
};
use wings_resources::SEQNUM_COLUMN_NAME;

pub type SeqnumRangeInclusive = RangeInclusive<u64>;

enum SeqnumPart {
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

pub fn validate_seqnum_filters(filters: &[Expr]) -> Result<SeqnumRangeInclusive, DataFusionError> {
    match filters
        .iter()
        .flat_map(find_seqnum_bound_in_filter)
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => Err(DataFusionError::Plan(format!(
            "No {0} filter provided. You must provide a lower and upper bound for {0}.",
            SEQNUM_COLUMN_NAME
        ))),
        [SeqnumPart::Eq(seqnum)] => Ok(*seqnum..=*seqnum),
        [bound0, bound1] => match (bound0, bound1) {
            (SeqnumPart::Lower(l), SeqnumPart::Upper(u)) => Ok(*l..=*u),
            (SeqnumPart::Upper(u), SeqnumPart::Lower(l)) => Ok(*l..=*u),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid {0} filter provided. You must provide a lower and upper bound for {0}.",
                SEQNUM_COLUMN_NAME
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "Invalid {0} filter provided. You must provide a lower and upper bound for {0}.",
            SEQNUM_COLUMN_NAME
        ))),
    }
}

fn find_seqnum_bound_in_filter(filter: &Expr) -> Option<SeqnumPart> {
    match filter {
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Gt,
        }) => seqnum_value(left, right).map(|seqnum| SeqnumPart::Lower(seqnum + 1)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::GtEq,
        }) => seqnum_value(left, right).map(SeqnumPart::Lower),
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Lt,
        }) => {
            let seqnum = seqnum_value(left, right)?;
            if seqnum == 0 {
                None
            } else {
                Some(SeqnumPart::Upper(seqnum - 1))
            }
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::LtEq,
        }) => seqnum_value(left, right).map(SeqnumPart::Upper),
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) => seqnum_value(left, right).map(SeqnumPart::Eq),
        _ => None,
    }
}

fn seqnum_value(left: &Expr, right: &Expr) -> Option<u64> {
    if left != &col(SEQNUM_COLUMN_NAME) {
        return None;
    }

    match right {
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
