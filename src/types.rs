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

fn expr_type(expr: &Expr, type_env: &HashMap<String, BaseType>) -> BaseType {
    match expr {
        Expr::Value(v) => value_type(v),
        Expr::Identifier(s) => *type_env.get(&format!("{}", s)).unwrap_or(&BaseType::Any),
        // TODO check if expr can be casted to data type
        Expr::Cast {
            expr: _expr,
            data_type,
        } => map_data_type(&data_type),
        // TODO extend
        _ => BaseType::Any,
    }
}

pub fn get_model_type(query: &Query, type_env: &im::HashMap<String, TableType>) -> TableType {
    // TODO extend with CTES
    match &query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            let mut is_open = false;

            let mut local_type_env = HashMap::new();

            // TODO optimize / simplify
            // (Re-)se immutable hashmap?
            for table in select.from.iter() {
                match &table.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => {
                        // TODO use alias to register name in environment
                        if let Some(ty) = type_env.get(&name.to_string()) {
                            match ty {
                                TableType::Open(s) => {
                                    for (s, ty) in s {
                                        local_type_env.insert(s.clone(), *ty);
                                    }
                                }
                                TableType::Closed(s) => {
                                    for (s, ty) in s {
                                        local_type_env.insert(s.clone(), *ty);
                                    }
                                }
                            }
                        }
                    }
                    sqlparser::ast::TableFactor::Derived { .. } => {
                        unimplemented!("Derived tables not supported")
                    }
                    sqlparser::ast::TableFactor::NestedJoin(_) => {
                        unimplemented!("nested join not supported")
                    }
                }
            }

            let items: Vec<(String, BaseType)> = select
                .projection
                .iter()
                .flat_map(|x| match x {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        Some((alias.to_string(), expr_type(expr, &local_type_env)))
                    }
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(id) => {
                            Some((id.to_string(), expr_type(expr, &local_type_env)))
                        }
                        _ => Some(("*".to_string(), BaseType::Any)),
                    },
                    // SelectItem::UnnamedExpr
                    // SelectItem::QualifiedWildcard
                    SelectItem::Wildcard => {
                        is_open = true;
                        // TODO: add everything to local type environment
                        None
                    }
                    _ => {
                        is_open = true;
                        None
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

#[test]
pub fn get_type_from_table() {
    let sql = "SELECT x AS a FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(
        &query,
        &im::HashMap::from(hashmap! {"t".to_string()=>
            TableType::Open(hashmap! {"x".to_string() => BaseType::Number})
        }),
    );

    assert_eq!(
        ty,
        TableType::Closed(hashmap! {"a".to_string() => BaseType::Number})
    )
}
