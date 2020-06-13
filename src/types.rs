use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;
use sqlparser::ast::Value;
use std::collections::HashMap;

#[derive(Debug, Eq, PartialEq, Clone)]
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
    Number,
    Float,
    // Add all known types
}

fn value_type(value: &Value) -> BaseType {
    match value {
        Value::Boolean(_) => BaseType::Boolean,
        Value::SingleQuotedString(_) => BaseType::String,
        Value::Number(_) => BaseType::Number,
        // TODO extend
        _ => BaseType::Any,
    }
}

fn map_data_type(data_type: &DataType) -> BaseType {
    match data_type {
        DataType::Float(_) => BaseType::Float,
        DataType::Boolean => BaseType::Boolean,
        // TODO: extend
        _ => BaseType::Any,
    }
}

fn expr_type(expr: &Expr) -> BaseType {
    match expr {
        Expr::Value(v) => value_type(v),
        Expr::Cast { expr, data_type } => map_data_type(&data_type),
        // TODO extend
        _ => BaseType::Any,
    }
}

pub fn get_model_type(query: &Query, type_env: &im::HashMap<String, TableType>) -> TableType {
    // TODO extend with CTES
    match &query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            let mut is_open = false;

            let mut identifiers = HashMap::new();

            for table in select.from.iter() {
                match &table.relation {
                    sqlparser::ast::TableFactor::Table {
                        name,
                        alias,
                        args,
                        with_hints,
                    } => {
                        println!("name, {}", name);
                        if let Some(ty) = type_env.get(&name.to_string()) {
                            match ty {
                                TableType::Open(s) => {
                                    for (s, ty) in s {
                                        identifiers.insert(s.clone(), ty);
                                    }
                                }
                                TableType::Closed(s) => {
                                    for (s, ty) in s {
                                        identifiers.insert(s.clone(), ty);
                                    }
                                }
                            }
                        }
                    }
                    sqlparser::ast::TableFactor::Derived {
                        lateral,
                        subquery,
                        alias,
                    } => unimplemented!("Derived tables not supported"),
                    sqlparser::ast::TableFactor::NestedJoin(_) => {
                        unimplemented!("nested join not supported")
                    }
                }
            }

            let items: Vec<(String, BaseType)> = select
                .projection
                .iter()
                .map(|x| match x {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        (alias.to_string(), expr_type(expr))
                    }
                    SelectItem::UnnamedExpr(expr) => match expr {
                        // Todo get type from environment
                        Expr::Identifier(id) => (
                            id.to_string(),
                            **identifiers.get(id).unwrap_or(&&BaseType::Any),
                        ),
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
        _ => unimplemented!("Not yet implemented"),
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
