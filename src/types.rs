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

pub fn expr_type(
    expr: &Expr,
    local_type_env: &HashMap<String, BaseType>,
    mut type_env: im::HashMap<String, TableType>,
    open: bool,
) -> Result<BaseType, String> {
    match expr {
        Expr::Value(v) => Ok(value_type(v)),
        Expr::Identifier(s) => {
            if open {
                Ok(*local_type_env
                    .get(&format!("{}", s))
                    .unwrap_or(&BaseType::Any))
            } else {
                local_type_env
                    .get(&format!("{}", s))
                    .copied()
                    .ok_or_else(|| format!("identifier {} not found", s))
            }
        }
        // TODO check if expr can be casted to data type
        Expr::Cast {
            expr: cast_expr,
            data_type,
        } => {
            let _ = expr_type(cast_expr, local_type_env, type_env, open)?;
            // TODO compatible / incompatible casting
            Ok(map_data_type(&data_type))
        }
        Expr::Exists(query) => {
            get_model_type(query, type_env)?;
            Ok(BaseType::Boolean)
        }
        Expr::UnaryOp { expr, op } => {
            let ty = expr_type(expr, local_type_env, type_env, open)?;

            match (op, ty) {
                (_, BaseType::Any) => Ok(BaseType::Any),
                (sqlparser::ast::UnaryOperator::Plus, BaseType::Number) => Ok(BaseType::Number),
                (sqlparser::ast::UnaryOperator::Plus, BaseType::Float) => Ok(BaseType::Float),
                (sqlparser::ast::UnaryOperator::Minus, BaseType::Number) => Ok(BaseType::Number),
                (sqlparser::ast::UnaryOperator::Minus, BaseType::Float) => Ok(BaseType::Float),
                (sqlparser::ast::UnaryOperator::Not, BaseType::Boolean) => Ok(BaseType::Boolean),
                (a, b) => Err(format!("Could not combine {:?} with {:?}", a, b)),
            }
        }
        // TODO extend
        _ => Ok(BaseType::Any),
    }
}

fn build_local_type_env(
    mut type_env: im::HashMap<String, TableType>,
    local_type_env: &mut HashMap<String, BaseType>,
    table_factor: &TableFactor,
) -> Result<(bool, im::HashMap<String, TableType>), String> {
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
            let (unknow_sources_2, n_type_env) =
                build_local_type_env(type_env.clone(), local_type_env, &join.relation)?;
            type_env = n_type_env;
            unknown_sources = unknown_sources || unknow_sources_2;

            for j in join.joins.iter() {
                let (unknow_sources_2, n_type_env) =
                    build_local_type_env(type_env.clone(), local_type_env, &j.relation)?;
                type_env = n_type_env;
                unknown_sources = unknown_sources || unknow_sources_2;
            }
        }
        TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } => {
            let ty = get_model_type(subquery, type_env.clone())?;
            type_env = type_env.update(format!("{}", alias.name), ty);
        }
        TableFactor::Derived { .. } => return Err("Derived tables should have alias".to_string()),
    }
    Ok((unknown_sources, type_env))
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
                    let (unknow_sources_2, n_type_env) =
                        build_local_type_env(type_env, &mut local_type_env, &join.relation)?;
                    type_env = n_type_env;
                    unknown_sources = unknown_sources || unknow_sources_2;
                }
                let (unknow_sources_2, n_type_env) =
                    build_local_type_env(type_env, &mut local_type_env, &table.relation)?;
                type_env = n_type_env;
                unknown_sources = unknown_sources || unknow_sources_2;
            }
            let mut items = vec![];

            for p in &select.projection {
                match p {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let ty =
                            expr_type(&expr, &local_type_env, type_env.clone(), unknown_sources)?;
                        items.push((alias.to_string(), ty));
                    }
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(id) => {
                            let ty = expr_type(
                                &expr,
                                &local_type_env,
                                type_env.clone(),
                                unknown_sources,
                            )?;
                            items.push((id.to_string(), ty))
                        }
                        _ => {}
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
