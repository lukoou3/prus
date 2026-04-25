use std::collections::HashMap;
use std::sync::Arc;
use arrow::compute::kernels::numeric::{add_wrapping, neg_wrapping};
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use itertools::Itertools;
use pest::{
    iterators::{Pair},
    Parser,
};
use pest_derive::Parser;
use serde_json::Value as JValue;

#[derive(Parser)]
#[grammar = "faker/expr.pest"]
pub struct ExprParser;

pub fn parse_expr(sql: &str) -> anyhow::Result<Expr> {
    let pair = ExprParser::parse(Rule::singleExpression, sql)?.next().unwrap();
    parse_expr_ast(pair)
}

pub fn parse_expr_ast(mut pair: Pair<Rule>) -> anyhow::Result<Expr> {
    loop {
        match pair.as_rule() {
            Rule::singleExpression =>
                pair = pair.into_inner().next().unwrap(),
            Rule::constant => return parse_constant(pair),
            Rule::columnReference => return Ok(Expr::Attribute(parse_identifier(pair)?.to_string())),
            Rule::addSubExpression | Rule::mulDivExpression  =>
                return parse_arithmetic_expression(pair),
            Rule::unaryExpression => return parse_unary_expression(pair),
            _ => {
                let mut pairs = pair.into_inner();
                if pairs.len() == 1 {
                    pair = pairs.next().unwrap();
                    // return parse_ast(pairs.next().unwrap());
                } else {
                    return Err(anyhow::anyhow!("Expected a single child but found {}:{}", pairs.len(), pairs.into_iter().map(|pair| pair.as_str()).join(", ")))
                }
            }
        }
    }
}

fn parse_unary_expression(pair: Pair<Rule>) -> anyhow::Result<Expr> {
    let mut pairs = pair.into_inner();
    if pairs.len() == 1 {
        Ok(parse_expr_ast(pairs.next().unwrap())?)
    } else {
        match pairs.next().unwrap().as_rule() {
            Rule::PLUS => {
                let expr = parse_expr_ast(pairs.next().unwrap())?;
                Ok(expr)
            },
            Rule::MINUS => {
                let expr = parse_expr_ast(pairs.next().unwrap())?;
                Ok(Expr::UnaryMinus(Box::new(expr)))
            },
            r => Err(anyhow::anyhow!("Expected a unary expression but found {:?}", r))
        }
    }
}

fn parse_arithmetic_expression(pair: Pair<Rule>) -> anyhow::Result<Expr> {
    let mut pairs = pair.into_inner();
    let mut left = parse_expr_ast(pairs.next().unwrap())?;
    let mut arithmetic_option = pairs.next();
    while let Some(arithmetic) = arithmetic_option {
        let right = parse_expr_ast(pairs.next().unwrap())?;
        match arithmetic.as_rule() {
            Rule::PLUS => left = Expr::BinaryOperator(Box::new(left), Operator::Plus, Box::new(right)),
            Rule::MINUS => left = Expr::BinaryOperator(Box::new(left), Operator::Minus, Box::new(right)),
            Rule::ASTERISK => left = Expr::BinaryOperator(Box::new(left), Operator::Multiply, Box::new(right)),
            Rule::SLASH => left = Expr::BinaryOperator(Box::new(left), Operator::Divide, Box::new(right)),
            Rule::PERCENT => left = Expr::BinaryOperator(Box::new(left), Operator::Modulo, Box::new(right)),
            _ => return Err(anyhow::anyhow!("Unexpected arithmetic {:?}", arithmetic))
        }
        arithmetic_option = pairs.next();
    }
    Ok(left)
}

fn parse_identifier(mut pair: Pair<Rule>) -> anyhow::Result<&str> {
    loop {
        match pair.as_rule() {
            Rule::unquotedIdentifier => return Ok(pair.as_str()),
            Rule::quotedIdentifier => {
                let s = pair.as_str();
                assert!(s.starts_with('`') && s.ends_with('`'));
                return Ok( &s[1..s.len() - 1]);
            },
            _ => {
                let mut pairs = pair.into_inner();
                if pairs.len() == 1 {
                    pair = pairs.next().unwrap();
                } else {
                    return Err(anyhow::anyhow!("identifier expected a single child but found {}:{}", pairs.len(), pairs.into_iter().map(|pair| pair.as_str()).join(", ")))
                }
            }
        }
    }
}

fn parse_constant(pair: Pair<Rule>) -> anyhow::Result<Expr> {
    let p = pair.clone().into_inner().next().unwrap();
    match p.as_rule() {
        Rule::number => {
            let num = p.into_inner().next().unwrap();
            match num.as_rule() {
                Rule::integerLiteral => {
                    let v:JValue = serde_json::from_str(num.as_str()).unwrap();
                    match v {
                        JValue::Number(n) => {
                            if n.is_f64() {
                                Ok(Expr::DoubleLiteral(n.as_f64().unwrap()))
                            } else if n.is_i64() {
                                let v = n.as_i64().unwrap();
                                if v <= i32::MAX as i64 {
                                    Ok(Expr::IntLiteral(v as i32))
                                } else {
                                    Ok(Expr::LongLiteral(v))
                                }
                            } else {
                                Err(anyhow::anyhow!("Unexpected parse_constant {:?}", n))
                            }
                        },
                        _ => Err(anyhow::anyhow!("Unexpected parse_constant {:?}", v))
                    }
                },
                Rule::bigIntLiteral => {
                    let s = num.as_str();
                    let s = &s[0.. s.len() - 1];
                    Ok(Expr::LongLiteral(s.parse::<i64>() ?))
                },
                Rule::decimalLiteral => {
                    let s = num.as_str();
                    Ok(Expr::DoubleLiteral(s.parse::<f64>() ?))
                },
                Rule::floatLiteral => {
                    let s = num.as_str();
                    let s = &s[0.. s.len() - 1];
                    Ok(Expr::FloatLiteral(s.parse::<f32>() ?))
                },
                Rule::doubleLiteral => {
                    let s = num.as_str();
                    let s = &s[0.. s.len() - 1];
                    Ok(Expr::DoubleLiteral(s.parse::<f64>() ?))
                },
                _ => Err(anyhow::anyhow!("Unexpected parse constant number {:?}", num))
            }
        },
        Rule::STRING => parse_string_constant(pair),
        _ => Err(anyhow::anyhow!("Unexpected parse_constant {:?}", p))
    }
}

fn parse_string_constant(pair: Pair<Rule>) -> anyhow::Result<Expr> {
    let s = pair.as_str();
    let s = &s[1.. s.len() - 1];

    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(escaped) = chars.next() {
                match escaped {
                    '\\' => result.push('\\'),
                    '\'' => result.push('\''),
                    '"' => result.push('"'),
                    'n' => result.push('\n'),
                    't' => result.push('\t'),
                    'r' => result.push('\r'),
                    _ => result.push(escaped), // 其他转义字符原样保留
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok(Expr::StringLiteral(result))
}

#[derive(Debug)]
pub enum Expr {
    Attribute(String),
    BinaryOperator(Box<Expr>, Operator, Box<Expr>),
    UnaryMinus(Box<Expr>),
    IntLiteral(i32),
    LongLiteral(i64),
    FloatLiteral(f32),
    DoubleLiteral(f64),
    StringLiteral(String),
}

impl Expr {

    pub fn eval(&self, columns: &HashMap<&str, ArrayRef>, row_count: usize) -> anyhow::Result<ArrayRef> {
        match self.eval_internal(columns, row_count)? {
            ColumnArray::ArrayRef(a) => Ok(a),
            ColumnArray::Literal(_) => Err(anyhow::anyhow!("not supported only literal expression")),
        }
    }
    fn eval_internal(&self, columns: &HashMap<&str, ArrayRef>, row_count: usize) -> anyhow::Result<ColumnArray> {
        match self {
            Expr::Attribute(col) => columns.get(col.as_str()).map(|a| ColumnArray::ArrayRef(a.clone())).ok_or_else(|| anyhow::anyhow!("column not found")),
            Expr::BinaryOperator(l, o, f) => {
                let left = l.eval_internal(columns, row_count)?;
                let right = f.eval_internal(columns, row_count)?;
                let (left, right) = match (left, right) {
                    (ColumnArray::ArrayRef(l), ColumnArray::ArrayRef(r)) => (l, r),
                    (ColumnArray::ArrayRef(l), ColumnArray::Literal(r)) => {
                        let r = Expr::create_array(l.data_type(), r, row_count)?;
                        (l, r)
                    },
                    (ColumnArray::Literal(l), ColumnArray::ArrayRef(r)) => {
                        let l = Expr::create_array(r.data_type(), l, row_count)?;
                        (l, r)
                    },
                    _ => return Err(anyhow::anyhow!("unsupported binary operator on two literal"))
                };

                match o {
                    Operator::Plus => Ok(ColumnArray::ArrayRef(add_wrapping(&left, &right)?)),
                    Operator::Minus => Ok(ColumnArray::ArrayRef(add_wrapping(&left, &right)?)),
                    Operator::Multiply => Ok(ColumnArray::ArrayRef(add_wrapping(&left, &right)?)),
                    Operator::Divide => Ok(ColumnArray::ArrayRef(add_wrapping(&left, &right)?)),
                    Operator::Modulo => Ok(ColumnArray::ArrayRef(add_wrapping(&left, &right)?)),
                    _ => Err(anyhow::anyhow!("Unexpected arithmetic {:?}", o))
                }
            },
            Expr::UnaryMinus(e) => {
                match e.eval_internal(columns, row_count)? {
                    ColumnArray::ArrayRef(array) => Ok(ColumnArray::ArrayRef(neg_wrapping(&array)?)),
                    ColumnArray::Literal(e) =>match e {
                        Expr::IntLiteral(v) => Ok(ColumnArray::Literal(Expr::IntLiteral(-v))),
                        Expr::LongLiteral(v) => Ok(ColumnArray::Literal(Expr::LongLiteral(-v))),
                        Expr::FloatLiteral(v) => Ok(ColumnArray::Literal(Expr::FloatLiteral(-v))),
                        Expr::DoubleLiteral(v) => Ok(ColumnArray::Literal(Expr::DoubleLiteral(-v))),
                        _ => unreachable!()
                    }
                }
            },
            Expr::IntLiteral(v) => Ok(ColumnArray::Literal(Expr::IntLiteral(*v))),
            Expr::LongLiteral(v) => Ok(ColumnArray::Literal(Expr::LongLiteral(*v))),
            Expr::FloatLiteral(v) => Ok(ColumnArray::Literal(Expr::FloatLiteral(*v))),
            Expr::DoubleLiteral(v) => Ok(ColumnArray::Literal(Expr::DoubleLiteral(*v))),
            Expr::StringLiteral(_) => Err(anyhow::anyhow!("Unexpected string literal")),
        }
    }

    fn create_array(data_type: &DataType,  r: Expr, len: usize) -> anyhow::Result<ArrayRef> {
        let r: ArrayRef = match data_type {
            DataType::Int32 => {
                let value = match r {
                    Expr::IntLiteral(v) => v,
                    Expr::LongLiteral(v) => v as i32,
                    Expr::FloatLiteral(v) => v as i32,
                    Expr::DoubleLiteral(v) => v as i32,
                    _ => unreachable!()
                };
                let mut array = Vec::with_capacity(len);
                array.resize(len, value);
                Arc::new(Int32Array::new(array.into(), None))
            },
            DataType::Int64 => {
                let value = match r {
                    Expr::IntLiteral(v) => v as i64,
                    Expr::LongLiteral(v) => v,
                    Expr::FloatLiteral(v) => v as i64,
                    Expr::DoubleLiteral(v) => v as i64,
                    _ => unreachable!()
                };
                let mut array = Vec::with_capacity(len);
                array.resize(len, value);
                Arc::new(Int64Array::new(array.into(), None))
            },
            DataType::Float32 => {
                let value = match r {
                    Expr::IntLiteral(v) => v as f32,
                    Expr::LongLiteral(v) => v as f32,
                    Expr::FloatLiteral(v) => v,
                    Expr::DoubleLiteral(v) => v as f32,
                    _ => unreachable!()
                };
                let mut array = Vec::with_capacity(len);
                array.resize(len, value);
                Arc::new(Float32Array::new(array.into(), None))
            },
            DataType::Float64 => {
                let value = match r {
                    Expr::IntLiteral(v) => v as f64,
                    Expr::LongLiteral(v) => v as f64,
                    Expr::FloatLiteral(v) => v as f64,
                    Expr::DoubleLiteral(v) => v ,
                    _ => unreachable!()
                };
                let mut array = Vec::with_capacity(len);
                array.resize(len, value);
                Arc::new(Float64Array::new(array.into(), None))
            },
            _ => return Err(anyhow::anyhow!("Unexpected type {:?}", data_type))
        };
        Ok(r)
    }

}

enum ColumnArray {
    ArrayRef(ArrayRef),
    Literal(Expr)
}

/// Operators applied to expressions
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
    /// Bitwise AND, like `&`
    BitAnd,
    /// Bitwise OR, like `|`
    BitOr,
    /// Bitwise XOR, like `^`
    BitXor,
    /// Bitwise left shift, like `<<`
    BitShiftLeft,
    /// Bitwise right shift, like `>>`
    BitShiftRight,
    /// Bitwise unsigned right shift, like `>>>`
    BitShiftRightUnsigned,
}

impl Operator {
    pub fn sql_operator(&self) -> &'static str {
        match self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
            Operator::And => "and",
            Operator::Or=> "or",
            Operator::BitAnd => "&",
            Operator::BitOr => "|",
            Operator::BitXor => "^",
            Operator::BitShiftLeft => "<<",
            Operator::BitShiftRight => ">>",
            Operator::BitShiftRightUnsigned => ">>>",
        }
    }
}