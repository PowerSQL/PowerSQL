use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;
use sqlparser::ast::TableFactor;
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
        DataType::Varchar(_) => BaseType::String,
        DataType::Text => BaseType::String,
        // TODO: extend
        _ => BaseType::Any,
    }
}

fn expr_type(
    expr: &Expr,
    type_env: &HashMap<String, BaseType>,
    open: bool,
) -> Result<BaseType, String> {
    match expr {
        Expr::Value(v) => Ok(value_type(v)),
        Expr::Identifier(s) => {
            if open {
                Ok(*type_env.get(&format!("{}", s)).unwrap_or(&BaseType::Any))
            } else {
                type_env
                    .get(&format!("{}", s))
                    .map(|x| *x)
                    .ok_or(format!("identifier {} not found", s).to_string())
            }
        }
        // TODO check if expr can be casted to data type
        Expr::Cast {
            expr: cast_expr,
            data_type,
        } => {
            let _ = expr_type(cast_expr, type_env, open)?;
            // TODO compatible / incompatible casting
            return Ok(map_data_type(&data_type));
        }
        // TODO extend
        _ => Ok(BaseType::Any),
    }
}

fn build_local_type_env(
    type_env: &im::HashMap<String, TableType>,
    local_type_env: &mut HashMap<String, BaseType>,
    table_factor: &TableFactor,
) -> bool {
    let mut unknown_sources = false;

    match table_factor {
        TableFactor::Table { name, .. } => {
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
            } else {
                unknown_sources = true;
            }
        }
        TableFactor::NestedJoin(join) => {
            unknown_sources =
                unknown_sources || build_local_type_env(type_env, local_type_env, &join.relation);
            unknown_sources = unknown_sources
                || join
                    .joins
                    .iter()
                    .map(|x| build_local_type_env(type_env, local_type_env, &x.relation))
                    .any(|x| x);
        }
        TableFactor::Derived { .. } => unimplemented!("Derived tables not supported"),
    }
    unknown_sources
}

pub fn get_model_type(
    query: &Query,
    mut type_env: im::HashMap<String, TableType>,
) -> Result<TableType, String> {
    for cte in query.ctes.iter() {
        let ty = get_model_type(&cte.query, type_env.clone())?;
        type_env = type_env.update(format!("{}", cte.alias), ty);
    }
    match &query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            let mut is_open = false;
            let mut unknown_sources = false;

            let mut local_type_env = HashMap::new();
            // TODO optimize / simplify
            // (Re-)use immutable hashmap?

            for table in select.from.iter() {
                for join in table.joins.iter() {
                    unknown_sources = unknown_sources
                        || build_local_type_env(&type_env, &mut local_type_env, &join.relation);
                }
                unknown_sources = unknown_sources
                    || build_local_type_env(&type_env, &mut local_type_env, &table.relation);
            }
            let mut items = vec![];

            for p in &select.projection {
                match p {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let ty = expr_type(&expr, &local_type_env, unknown_sources)?;
                        items.push((alias.to_string(), ty));
                    }
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(id) => {
                            let ty = expr_type(&expr, &local_type_env, unknown_sources)?;
                            items.push((id.to_string(), ty))
                        }
                        _ => {
                            Err("Unnamed expressions not supported")?;
                        }
                    },
                    _ => {
                        is_open = true;
                    }
                }
            }

            let map = items
                .iter()
                .filter(|(x, _)| x != "*")
                .map(|(x, ty)| (x.to_string(), *ty))
                .collect();
            if is_open {
                Ok(TableType::Open(map))
            } else {
                Ok(TableType::Closed(map))
            }
        }
        sqlparser::ast::SetExpr::Query(query) => get_model_type(query, type_env),
        _ => Err("Statement not yet implemented".to_string()),
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
    let ty = get_model_type(&query, im::HashMap::new());

    assert_eq!(
        ty,
        Ok(TableType::Closed(
            hashmap! {"a".to_string() => BaseType::Any}
        ))
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
    let ty = get_model_type(&query, im::HashMap::new());

    assert_eq!(ty, Ok(TableType::Open(HashMap::new())))
}

#[test]
pub fn get_model_type_test_constants() {
    let sql = "SELECT '1' AS a FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, im::HashMap::new());

    assert_eq!(
        ty,
        Ok(TableType::Closed(
            hashmap! {"a".to_string() => BaseType::String}
        ))
    )
}

#[test]
pub fn get_model_type_test_() {
    let sql = "WITH t AS (SELECT '1' AS a from q) SELECT a FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, im::HashMap::new());

    assert_eq!(
        ty,
        Ok(TableType::Closed(
            hashmap! {"a".to_string() => BaseType::String}
        ))
    )
}
#[test]
pub fn get_model_type_join() {
    let sql =
        "WITH t AS (SELECT '1' AS a), u AS (SELECT '1' AS b) SELECT a, b FROM t JOIN u ON 1=1";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, im::HashMap::new());

    assert_eq!(
        ty,
        Ok(TableType::Closed(
            hashmap! {"a".to_string() => BaseType::String, "b".to_string() => BaseType::String},
        ))
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
        im::HashMap::from(hashmap! {"t".to_string()=>
            TableType::Open(hashmap! {"x".to_string() => BaseType::Number})
        }),
    );

    assert_eq!(
        ty,
        Ok(TableType::Closed(
            hashmap! {"a".to_string() => BaseType::Number}
        ))
    )
}

#[test]
pub fn get_cast_type() {
    let sql = "SELECT CAST(x AS VARCHAR) AS b FROM t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query, im::HashMap::from(hashmap! {}));

    assert_eq!(
        ty,
        Ok(TableType::Closed(
            hashmap! {"b".to_string() => BaseType::String}
        ))
    )
}
