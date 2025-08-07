use std::ops::Deref;

use datafusion::{
    logical_expr::{BinaryExpr, Operator},
    prelude::{Expr, col},
    scalar::ScalarValue,
};

pub const TOPIC_NAME_COLUMN: &str = "topic";

pub fn find_topic_name_in_filters(filters: &[Expr]) -> Option<Vec<String>> {
    let filters: Vec<_> = filters.iter().flat_map(find_topic_name_in_filter).collect();
    if filters.is_empty() {
        None
    } else {
        Some(filters)
    }
}

fn find_topic_name_in_filter(filter: &Expr) -> Option<String> {
    match filter {
        Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) => {
            if left.deref() != &col(TOPIC_NAME_COLUMN) {
                return None;
            }

            match right.deref() {
                Expr::Literal(
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)),
                    _,
                ) => Some(s.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}
