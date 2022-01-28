use crate::lexer::SqlValue;
use crate::table::Table;

#[derive(Debug)]
pub enum CmpOperator {
    Less,
    Greater,
    Equals,
    NotEquals,
    LessEquals,
    GreaterEquals,
}

impl CmpOperator {
    pub fn apply(&self, left: &SqlValue, right: &SqlValue) -> Result<bool, String> {
        match left {
            SqlValue::Integer(lvalue) => {
                match right {
                    SqlValue::Integer(rvalue) => Ok(self.cmp_ord(lvalue, rvalue)),
                    SqlValue::Null => Ok(false),
                    _ => Err(format!("cannot compare {:?} with number", right)),
                }

            },
            SqlValue::String(lvalue) | SqlValue::Identificator(lvalue) => {
                match self {
                    Self::Equals | Self::NotEquals => {
                        match right {
                            SqlValue::Integer(_rvalue) =>  Err(format!("cannot compare {} with number", lvalue)),
                            SqlValue::String(rvalue) | SqlValue::Identificator(rvalue) => self.cmp_eq(lvalue, rvalue),
                            SqlValue::Null => Ok(false),
                        }
                    },
                    _ => Err(format!("string {} can only be compared with other values with '=' or '<>'", lvalue)),
                }
            },
            SqlValue::Null => Ok(false)
        }
    }

    fn cmp_eq<Stringlike>(&self, left: Stringlike, right: Stringlike) -> Result<bool, String>
    where
        Stringlike: PartialEq + std::fmt::Display
    {
        match self {
            Self::Equals => Ok(left == right),
            Self::NotEquals => Ok(left != right),
            _ => Err(format!("cannot compare {} with {}", left, right)),
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
        }
    }
}

#[derive(Debug)]
pub struct WhereClause {
    pub left_value: SqlValue,
    pub right_value: SqlValue,
    pub operator: CmpOperator,
}

impl WhereClause {
    pub fn build_filter<'a>(&'a self, table: &'a Table) -> Box<dyn Fn(&'a Vec<SqlValue>) -> Result<bool, String> + 'a> {
        let get_left_value = self.build_value_getter(table, &self.left_value);
        let get_right_value = self.build_value_getter(table, &self.right_value);

        Box::new(move |row: &Vec<SqlValue>| {
            self.operator.apply(&get_left_value(row), &get_right_value(row))
        })
    }


    fn build_value_getter<'a>(&'a self, table: &'a Table, value: &'a SqlValue) -> Box<dyn Fn(&'a Vec<SqlValue>) -> SqlValue + 'a> {
        let dummy_getter = |_row| value.clone();
        let table_name = table.name.as_str();
        let string_value = value.to_string();
        let column_name = {
            let splitted_identificator: Vec<&str> = string_value.split('.').collect();
            match splitted_identificator.len() {
                1 => string_value.as_str(),
                2 => {
                    if !splitted_identificator[0].eq(table_name) {
                        return Box::new(dummy_getter);
                    } else {
                        splitted_identificator[1]
                    }
                },
                _ => return Box::new(dummy_getter),
            }
        };

        if let Some(column_index) = table.column_index(column_name) {
           Box::new(move |row: &Vec<SqlValue>| row[column_index].clone())
        } else {
           Box::new(dummy_getter)
        }
    }
}
