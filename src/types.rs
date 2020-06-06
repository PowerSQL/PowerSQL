use sqlparser::ast::Expr;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;
use sqlparser::ast::Value;

use std::collections::HashMap;
#[derive(Debug, Eq, PartialEq)]
pub enum TableType {
    // SELECT *,[...] FROM t
    Open(HashMap<String, BaseType>),
    // SELECT a, b, c FROM ts
    Closed(HashMap<String, BaseType>),
}
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum BaseType {
    Any,
    String,
    Boolean,
    // Add all known types
}

fn value_type(value: &Value) -> BaseType {
    match value {
        Value::Boolean(_) => BaseType::Boolean,
        Value::SingleQuotedString(_) => BaseType::String,
        // TODO extend
        _ => BaseType::Any,
    }
}

fn expr_type(expr: &Expr) -> BaseType {
    match expr {
        Expr::Value(v) => value_type(v),
        // TODO extend
        _ => BaseType::Any,
    }
}

pub fn get_model_type(query: &Query, _type_env: &im::HashMap<String, TableType>) -> TableType {
    // TODO extend with CTES
    match &query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            let mut is_open = false;
            let items: Vec<(String, BaseType)> = select
                .projection
                .iter()
                .map(|x| match x {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        (alias.to_string(), expr_type(expr))
                    }
                    SelectItem::UnnamedExpr(expr) => match expr {
                        // Todo get type from environment
                        Expr::Identifier(id) => (id.to_string(), BaseType::Any),
                        _ => ("*".to_string(), BaseType::Any),
                    },
                    // SelectItem::UnnamedExpr
                    // SelectItem::QualifiedWildcard
                    // SelectItem::Wildcard
                    // TODO wildcard from closed Table is closed
                    _ => {
                        is_open = true;
                        ("*".to_string(), BaseType::Any)
                    }
                })
                .collect();

            let map = items
                .iter()
                .filter(|(x, _)| x != "*")
                .map(|(x, ty)| (x.to_string(), *ty))
                .collect();
            if is_open {
                TableType::Open(map)
            } else {
                TableType::Closed(map)
            }
        }
        _ => TableType::Open(HashMap::new()),
    }
}

#[cfg(test)]
use super::parser::PowerSqlDialect;
#[cfg(test)]
use sqlparser::parser::Parser;
#[cfg(test)]
use sqlparser::tokenizer::Tokenizer;

#[test]
pub fn get_model_type_test() {
    let sql = "SELECT a FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, &im::HashMap::new());

    assert_eq!(
        ty,
        TableType::Closed(hashmap! {"a".to_string() => BaseType::Any})
    )
}

#[test]
pub fn get_model_type_wildcard() {
    let sql = "SELECT * FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, &im::HashMap::new());

    assert_eq!(ty, TableType::Open(HashMap::with_capacity(0)))
}

#[test]
pub fn get_model_type_test_constants() {
    let sql = "SELECT '1' AS a FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, &im::HashMap::new());

    assert_eq!(
        ty,
        TableType::Closed(hashmap! {"a".to_string() => BaseType::String})
    )
}
