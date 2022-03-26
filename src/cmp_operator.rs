use std::fmt;

use crate::lexer::SqlValue;
use crate::execution_error::ExecutionError;

#[derive(Debug, Clone, Copy)]
pub enum CmpOperator {
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
    IsNull,
}

impl<'a> fmt::Display for CmpOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Less => write!(f, "<"),
            Self::Greater => write!(f, ">"),
            Self::Equals => write!(f, "="),
            Self::NotEquals => write!(f, "<>"),
            Self::LessEquals => write!(f, "<="),
            Self::GreaterEquals => write!(f, ">="),
            Self::IsNull => write!(f, "IS NULL"),
        }
    }
}

impl CmpOperator {
    pub fn apply(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, ExecutionError> {
        match self {
            Self::IsNull => Ok(left == &SqlValue::Null),
            _ => self.apply_cmp(left, right),
        }
    }

    pub fn apply_cmp(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, ExecutionError> {
        match left {
            SqlValue::Integer(l_int) => self.cmp_int_to_value(*l_int, right),
            SqlValue::String(ref l_string) | SqlValue::Identificator(ref l_string) =>
                self.cmp_string_to_value(l_string, right),
            SqlValue::Null => Ok(false),
        }
    }

    fn cmp_int_to_value(&self, l_int: i64, r_value: &SqlValue) -> Result<bool, ExecutionError> {
        match r_value {
            SqlValue::Integer(r_int) => Ok(self.cmp_ord(l_int, *r_int)),
            SqlValue::Null => Ok(false),
            _ => Err(ExecutionError::CannotCompareWithNumber(r_value.clone())),
        }
    }

    fn cmp_string_to_value(&self, l_string: &str, r_value: &SqlValue) -> Result<bool, ExecutionError> {
        match self {
            Self::Equals | Self::NotEquals => {
                match r_value {
                    SqlValue::Integer(_r_int) =>
                        Err(ExecutionError::CannotCompareWithNumber(SqlValue::String(l_string.to_string()))),
                    SqlValue::String(ref r_string) | SqlValue::Identificator(ref r_string) => self.cmp_strings(l_string, r_string),
                    SqlValue::Null => Ok(false),
                }
            },
            _ => Err(ExecutionError::OperatorNotApplicable {
                    operator: *self,
                    lvalue: SqlValue::String(l_string.to_string()),
                    rvalue: r_value.clone(),
                })
        }
    }

    fn cmp_strings(&self, left: &str, right: &str) -> Result<bool, ExecutionError> {
        match self {
            Self::Equals => Ok(left == right),
            Self::NotEquals => Ok(left != right),
            _ => Err(ExecutionError::NonEqualityComparisonWithStrings { operator: *self, lvalue: left.to_string(), rvalue: right.to_string() })
        }
    }

    fn cmp_ord<Number>(&self, left: Number, right: Number) -> bool
    where
        Number: PartialOrd
    {
        match self {
            Self::Less => left < right,
            Self::Greater => left > right,
            Self::Equals => left == right,
            Self::NotEquals => left != right,
            Self::LessEquals => left <= right,
            Self::GreaterEquals => left >= right,
            Self::IsNull => panic!("IS NULL cannot be handled in cmp_ord"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_int_to_sql_int() {
        let left = SqlValue::Integer(1);
        let right = SqlValue::Integer(2);

        assert_eq!(CmpOperator::Less.apply(&left, &right).unwrap(), true);
        assert_eq!(CmpOperator::Greater.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::Equals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::NotEquals.apply(&left, &right).unwrap(), true);
        assert_eq!(CmpOperator::GreaterEquals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::LessEquals.apply(&left, &right).unwrap(), true);
        assert_eq!(CmpOperator::IsNull.apply(&left, &right).unwrap(), false);

        let left = SqlValue::Integer(2);
        let right = SqlValue::Integer(2);

        assert_eq!(CmpOperator::Less.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::Greater.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::Equals.apply(&left, &right).unwrap(), true);
        assert_eq!(CmpOperator::NotEquals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::LessEquals.apply(&left, &right).unwrap(), true);
        assert_eq!(CmpOperator::GreaterEquals.apply(&left, &right).unwrap(), true);
        assert_eq!(CmpOperator::IsNull.apply(&left, &right).unwrap(), false);
    }

    #[test]
    fn sql_int_to_sql_string() {
        let left = SqlValue::Integer(1);
        let right = SqlValue::String("1".to_string());

        assert!(CmpOperator::Less.apply(&left, &right).is_err());
        assert!(CmpOperator::Greater.apply(&left, &right).is_err());
        assert!(CmpOperator::Equals.apply(&left, &right).is_err());
        assert!(CmpOperator::LessEquals.apply(&left, &right).is_err());
        assert!(CmpOperator::GreaterEquals.apply(&left, &right).is_err());
        assert!(CmpOperator::NotEquals.apply(&left, &right).is_err());
        assert_eq!(CmpOperator::IsNull.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::IsNull.apply(&right, &left).unwrap(), false);
    }

    #[test]
    fn sql_int_to_null() {
        let left = SqlValue::Integer(1);
        let right = SqlValue::Null;

        assert_eq!(CmpOperator::Less.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::Greater.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::Equals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::NotEquals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::GreaterEquals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::LessEquals.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::IsNull.apply(&left, &right).unwrap(), false);
        assert_eq!(CmpOperator::IsNull.apply(&right, &left).unwrap(), true);
    }
}
