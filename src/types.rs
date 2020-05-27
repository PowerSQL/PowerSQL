use sqlparser::ast::Expr;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;

use std::collections::HashMap;
#[derive(Debug, Eq, PartialEq)]
pub enum TableType {
    // SELECT *,[...] FROM t
    Open(HashMap<String, BaseType>),
    // SELECT a, b, c FROM ts
    Closed(HashMap<String, BaseType>),
}
#[derive(Debug, Eq, PartialEq)]
pub enum BaseType {
    Any,
    String,
    // Add all known types
}

pub fn get_model_type(query: &Query) -> TableType {
    // TODO extend with CTES
    match &query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            let mut is_open = false;
            let items: Vec<String> = select
                .projection
                .iter()
                .map(|x| match x {
                    SelectItem::ExprWithAlias { expr, alias } => alias.to_string(),
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(id) => id.to_string(),
                        _ => "*".to_string(),
                    },
                    //SelectItem::ExprWithAlias
                    // SelectItem::UnnamedExpr
                    // SelectItem::QualifiedWildcard
                    // SelectItem::Wildcard
                    // TODO wildcard from closed Table is closed
                    _ => {
                        is_open = true;
                        "*".to_string()
                    }
                })
                .collect();

            let is_open = items.iter().any(|x| *x == "*");

            let map = items
                .iter()
                .map(|x| (x.to_string(), BaseType::Any))
                .collect();
            return if is_open {
                TableType::Open(map)
            } else {
                TableType::Closed(map)
            };
        }
        _ => TableType::Open(HashMap::new()),
    }
}

#[cfg(test)]
use super::parser::PowerSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

#[test]
pub fn get_model_type_test() {
    let sql = "select a from t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query().unwrap();
    let ty = get_model_type(&query);

    assert_eq!(
        ty,
        TableType::Closed(hashmap! {"a".to_string() => BaseType::Any})
    )
}
