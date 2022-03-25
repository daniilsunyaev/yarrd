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
            SqlValue::Integer(lvalue) => {
                match right {
                    SqlValue::Integer(rvalue) => Ok(self.cmp_ord(lvalue, rvalue)),
                    SqlValue::Null => Ok(false),
                    _ => Err(ExecutionError::CannotCompareWithNumber(right.clone())),
                }

            },
            SqlValue::String(ref lvalue) | SqlValue::Identificator(ref lvalue) => {
                match self {
                    Self::Equals | Self::NotEquals => {
                        match right {
                            SqlValue::Integer(_rvalue) =>  Err(ExecutionError::CannotCompareWithNumber(left.clone())),
                            SqlValue::String(ref rvalue) | SqlValue::Identificator(ref rvalue) => self.cmp_eq(lvalue, rvalue),
                            SqlValue::Null => Ok(false),
                        }
                    },
                    _ => Err(ExecutionError::OperatorNotApplicable { operator: *self, lvalue: left.clone(), rvalue: right.clone() })
                }
            },
            SqlValue::Null => Ok(false),
        }
    }

    fn cmp_eq(&self, left: &str, right: &str) -> Result<bool, ExecutionError> {
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
