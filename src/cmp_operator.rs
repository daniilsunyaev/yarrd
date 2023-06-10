use std::fmt;
use std::error::Error;

use crate::lexer::SqlValue;

#[derive(Debug)]
pub enum CmpError {
    CannotCompareWithInteger(SqlValue),
    CannotCompareWithFloat(SqlValue),
    NonEqualityComparisonWithStrings { operator: CmpOperator, lvalue: String, rvalue: String },
    OperatorNotApplicable { operator: CmpOperator, lvalue: SqlValue, rvalue: SqlValue },
}

impl fmt::Display for CmpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            Self::CannotCompareWithInteger(sql_value) => format!("cannot compare '{}' with integer", sql_value),
            Self::CannotCompareWithFloat(sql_value) => format!("cannot compare '{}' with float", sql_value),
            Self::OperatorNotApplicable { operator, lvalue, rvalue } =>
                format!("operator '{}' cannot be applied to values '{}' and {}",
                        operator, lvalue, rvalue),
            Self::NonEqualityComparisonWithStrings { operator, lvalue, rvalue } =>
                format!("non-equality operator '{}' cannot be applied to strings '{}' and {}, only '=' or '<>' can be used",
                        operator, lvalue, rvalue),
        };

        write!(f, "{}", message)
    }
}

impl Error for CmpError { }

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CmpOperator {
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
    IsNull,
}

impl fmt::Display for CmpOperator {
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
    pub fn apply(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, CmpError> {
        match self {
            Self::IsNull => Ok(left == &SqlValue::Null),
            _ => self.apply_cmp(left, right),
        }
    }

    pub fn apply_cmp(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, CmpError> {
        match left {
            SqlValue::Integer(l_int) => self.cmp_int_to_value(*l_int, right),
            SqlValue::Float(l_float) => self.cmp_float_to_value(*l_float, right),
            SqlValue::String(ref l_string) | SqlValue::Identificator(ref l_string) =>
                self.cmp_string_to_value(l_string, right),
            SqlValue::Null => Ok(false),
        }
    }

    fn cmp_int_to_value(&self, l_int: i64, r_value: &SqlValue) -> Result<bool, CmpError> {
        match r_value {
            SqlValue::Integer(r_int) => Ok(self.cmp_ord(l_int, *r_int)),
            SqlValue::Null => Ok(false),
            _ => Err(CmpError::CannotCompareWithInteger(r_value.clone())),
        }
    }

    fn cmp_float_to_value(&self, l_float: f64, r_value: &SqlValue) -> Result<bool, CmpError> {
        match r_value {
            SqlValue::Float(r_float) => Ok(self.cmp_ord(l_float, *r_float)),
            SqlValue::Null => Ok(false),
            _ => Err(CmpError::CannotCompareWithFloat(r_value.clone())),
        }
    }

    fn cmp_string_to_value(&self, l_string: &str, r_value: &SqlValue) -> Result<bool, CmpError> {
        match self {
            Self::Equals | Self::NotEquals => {
                match r_value {
                    SqlValue::Integer(_) =>
                        Err(CmpError::CannotCompareWithInteger(SqlValue::String(l_string.to_string()))),
                    SqlValue::Float(_) =>
                        Err(CmpError::CannotCompareWithFloat(SqlValue::String(l_string.to_string()))),
                    SqlValue::String(ref r_string) | SqlValue::Identificator(ref r_string) => self.cmp_strings(l_string, r_string),
                    SqlValue::Null => Ok(false),
                }
            },
            _ => Err(CmpError::OperatorNotApplicable {
                    operator: *self,
                    lvalue: SqlValue::String(l_string.to_string()),
                    rvalue: r_value.clone(),
                })
        }
    }

    fn cmp_strings(&self, left: &str, right: &str) -> Result<bool, CmpError> {
        match self {
            Self::Equals => Ok(left == right),
            Self::NotEquals => Ok(left != right),
            _ => Err(CmpError::NonEqualityComparisonWithStrings { operator: *self, lvalue: left.to_string(), rvalue: right.to_string() })
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

        let left = SqlValue::Float(2.0);
        let right = SqlValue::Float(2.0);

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
    fn sql_int_to_float() {
        let left = SqlValue::Integer(1);
        let right = SqlValue::Float(1.0);

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
