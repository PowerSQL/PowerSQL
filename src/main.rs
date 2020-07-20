mod execute;
mod parser;
mod types;
use execute::Executor;
use parser::PowerSqlDialect;
use serde_derive::Deserialize;
use sqlparser::ast::{
    Cte, Expr, Function, ListAgg, Query, SelectItem, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use structopt::StructOpt;
use walkdir::WalkDir;

#[derive(Deserialize, Debug)]
struct PowerSqlConfig {
    project: Project,
}
#[derive(Deserialize, Debug)]
struct Project {
    name: String,
    models: Vec<String>,
    tests: Option<Vec<String>>,
}
#[derive(Debug, StructOpt)]
enum Command {
    Check,
    Run,
    Test {
        #[structopt(long)]
        fail_fast: bool,
    },
    //Docs,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "PowerSQL", about = "The data tool")]
struct Opt {
    #[structopt(subcommand, name = "CMD")]
    command: Command,
}

// TODO don't pass mutable vec
fn get_refs_cte(cte: &Cte, vec: &mut Vec<String>) {
    get_refs(&cte.query, vec)
}

fn get_refs_table_factor(table_factor: &TableFactor, vec: &mut Vec<String>) {
    match table_factor {
        TableFactor::Table { name, .. } => vec.push(format!("{}", name)),
        TableFactor::NestedJoin(nested_join) => {
            get_refs_table_factor(&nested_join.relation, vec);

            for join in nested_join.joins.iter() {
                get_refs_table_factor(&join.relation, vec);
            }
        }
        TableFactor::Derived {
            subquery: query, ..
        } => {
            get_refs(query, vec);
        }
    }
}

fn get_refs_set_expr(body: &SetExpr, vec: &mut Vec<String>) {
    match body {
        SetExpr::Query(q) => get_refs(q, vec),
        SetExpr::Select(select) => {
            select
                .from
                .iter()
                .for_each(|table| get_refs_table_factor(&table.relation, vec));

            select.projection.iter().for_each(|x| match x {
                SelectItem::ExprWithAlias { expr, .. } => get_refs_expr(expr, vec),
                SelectItem::UnnamedExpr(expr) => get_refs_expr(expr, vec),
                _ => {}
            });
        }
        _ => {}
    }
}

fn get_refs(query: &Query, vec: &mut Vec<String>) {
    query.ctes.iter().for_each(|x| get_refs_cte(x, vec));
    get_refs_set_expr(&query.body, vec);
}

fn get_refs_expr(expr: &Expr, vec: &mut Vec<String>) {
    match expr {
        Expr::Between { low, high, .. } => {
            get_refs_expr(low, vec);
            get_refs_expr(high, vec);
        }
        Expr::BinaryOp { left, right, .. } => {
            get_refs_expr(left, vec);
            get_refs_expr(right, vec);
        }
        Expr::Cast { expr, .. } => {
            get_refs_expr(expr, vec);
        }
        Expr::Collate { expr, .. } => {
            get_refs_expr(expr, vec);
        }
        Expr::Exists(query) => get_refs(query, vec),
        Expr::Extract { expr, .. } => get_refs_expr(expr, vec),
        Expr::Function(Function { args, .. }) => {
            for arg in args {
                get_refs_expr(arg, vec);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            get_refs_expr(expr, vec);
            get_refs(subquery, vec);
        }
        Expr::IsNotNull(expr) => {
            get_refs_expr(expr, vec);
        }
        Expr::IsNull(expr) => {
            get_refs_expr(expr, vec);
        }
        Expr::ListAgg(ListAgg { expr, .. }) => {
            get_refs_expr(expr, vec);
        }
        Expr::Nested(expr) => {
            get_refs_expr(expr, vec);
        }
        Expr::Subquery(query) => get_refs(query, vec),
        Expr::UnaryOp { expr, .. } => get_refs_expr(expr, vec),
        _ => {}
    }
}

fn load_asts(models: &[String]) -> Result<HashMap<String, Statement>, String> {
    let mut res = HashMap::new();
    for path in models.iter() {
        let sql = fs::read_to_string(path).unwrap();
        let statements = Parser::parse_sql(&PowerSqlDialect {}, &sql)
            .map_err(|err| format!("Parse Error in {}: {}", path, err))?;

        for statement in statements {
            let name = match &statement {
                Statement::CreateView { name, .. } => format!("{}", name),
                Statement::CreateTable {
                    name,
                    query: Some(_),
                    ..
                } => format!("{}", name),
                _ => unimplemented!("Only (materialized) view and create table as supported "),
            };
            res.insert(name, statement);
        }
    }
    Ok(res)
}

fn load_tests(models: &[String]) -> Result<Vec<Statement>, String> {
    let mut res = vec![];
    for path in models.iter() {
        let sql = fs::read_to_string(path).unwrap();

        let statements = Parser::parse_sql(&PowerSqlDialect {}, &sql)
            .map_err(|err| format!("Parse Error in {}: {}", path, err))?;

        for statement in statements {
            let query = match statement {
                assert
                @
                Statement::Assert {
                    message: Some(_), ..
                } => assert,
                _ => unimplemented!("Only assert statements are supported in test files"),
            };
            res.push(query)
        }
    }
    Ok(res)
}

fn get_query(statement: &Statement) -> &Query {
    match statement {
        Statement::CreateView { query, .. } => query,
        Statement::CreateTable {
            query: Some(query), ..
        } => query,
        Statement::Query(query) => query,
        _ => unreachable!("Expected view, table, of query in fn get_query"),
    }
}

fn get_refs_statement(statement: &Statement, vec: &mut Vec<String>) {
    match statement {
        Statement::CreateView { query, .. } => get_refs(query, vec),
        Statement::CreateTable {
            query: Some(query), ..
        } => get_refs(query, vec),
        // Statement::Query(query) => {
        //     get_refs(query, vec);
        // }
        // Statement::Assert { condition, .. } => {
        //     get_refs_expr(condition, vec);
        // }
        _ => unreachable!("Expected view or table in fn get_query"),
    }
}

fn get_dependencies(asts: &HashMap<String, Statement>) -> HashMap<String, Vec<String>> {
    asts.iter()
        .map(|(src, stmt)| {
            let mut x = vec![];
            get_refs_statement(&stmt, &mut x);
            (
                src.clone(),
                x.iter()
                    .filter(|elem| asts.contains_key(*elem))
                    .cloned()
                    .collect(),
            )
        })
        .collect()
}

fn detect_cycles(deps: &HashMap<String, Vec<String>>) -> Result<(), String> {
    let mut visited_all = HashSet::new();
    for (model, model_deps) in deps.iter() {
        let mut visited = HashSet::new();
        let mut stack = model_deps.clone();

        visited.insert(model);
        while let Some(x) = stack.pop() {
            if visited_all.contains(&x) {
                continue;
            }
            visited_all.insert(x.clone());

            let d = deps.get(&x).ok_or(format!("Model {} not found", &x))?;

            for i in d.iter() {
                if visited.contains(i) {
                    return Err(format!("Loop detected while checking model {}", model));
                }
                visited.insert(i);
            }
            stack.extend(d.clone());
        }
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct ModelNode {
    live_parents: usize,
    next_nodes: Vec<String>,
}

fn build_graph(deps: &HashMap<String, Vec<String>>) -> Result<HashMap<&str, ModelNode>, String> {
    let mut graph = HashMap::new();

    let mut nodes = Vec::new();
    for (model, model_deps) in deps.iter() {
        // for each model collect number of parents
        graph.entry(model.as_str()).or_insert(ModelNode {
            live_parents: 0,
            next_nodes: vec![],
        });

        for m in model_deps {
            nodes.push((model.as_str(), m.as_str()));
        }
    }

    for (to, from) in nodes {
        let x = graph.get_mut(from).unwrap();
        x.next_nodes.push(to.to_string());

        let mut y = graph.get_mut(to).unwrap();
        y.live_parents += 1;
    }

    Ok(graph)
}

fn find_test_files(tests: Option<Vec<String>>) -> Vec<String> {
    let mut tests_models = vec![];
    if let Some(tests) = tests {
        for dir in tests {
            for entry in WalkDir::new(dir.to_string()) {
                let entry = entry.unwrap();
                if let Some(ext) = entry.path().extension() {
                    {
                        if ext == "sql" {
                            tests_models.push(entry.path().to_str().unwrap().to_string());
                        }
                    }
                }
            }
        }
    }
    tests_models
}

#[cfg(feature = "bigquery")]
async fn get_executor() -> Result<execute::BigqueryRunner, String> {
    execute::BigqueryRunner::new().await
}

#[cfg(feature = "postgres")]
async fn get_executor() -> Result<execute::Postgres, String> {
    execute::Postgres::new().await
}

fn expr_to_message(expr: &Expr) -> &str {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
        _ => "",
    }
}

#[tokio::main]
pub async fn main() -> Result<(), String> {
    let opt = Opt::from_args();

    // Load project
    let contents = fs::read_to_string("powersql.toml").expect("No powersql.toml file found");
    let config = toml::from_str(&contents);
    let config: PowerSqlConfig = match config {
        Err(x) => return Err(x.to_string()),
        Ok(conf) => conf,
    };
    let mut models = vec![];

    for dir in config.project.models {
        for entry in WalkDir::new(dir.to_string()) {
            let entry = entry.unwrap();
            if let Some(ext) = entry.path().extension() {
                {
                    if ext == "sql" {
                        models.push(entry.path().to_str().unwrap().to_string());
                    }
                }
            }
        }
    }
    let asts = load_asts(&models)?;
    let dependencies: HashMap<String, Vec<String>> = get_dependencies(&asts);
    detect_cycles(&dependencies)?;

    match opt.command {
        Command::Check => {
            let mut graph = build_graph(&dependencies)?;

            let mut nodes: Vec<_> = graph
                .iter()
                .filter(|(_m, node)| node.live_parents == 0)
                .map(|(x, _)| (*x).to_string())
                .collect();

            let mut ty_env = im::HashMap::new();

            while let Some(m) = nodes.pop() {
                println!("Checking {}", m);

                let node = graph.get(m.as_str()).unwrap().clone();
                let ty = types::get_model_type(get_query(asts.get(&m).unwrap()), ty_env.clone())?;
                println!("{} {:?}", m, ty);
                ty_env = ty_env.update(m.to_string(), ty);
                println!("ty_env {:?}", ty_env);
                for n in node.next_nodes.iter() {
                    let mut node = graph.get_mut(n.as_str()).unwrap();
                    node.live_parents -= 1;
                    if node.live_parents == 0 {
                        nodes.push(n.to_string());
                    }
                }
            }
            let test_models = find_test_files(config.project.tests);
            let tests = load_tests(&test_models)?;

            for test in tests {
                types::get_model_type(get_query(&test), ty_env.clone())?;
            }
        }
        Command::Run => {
            let mut graph = build_graph(&dependencies)?;

            let mut nodes: Vec<_> = graph
                .iter()
                .filter(|(_m, node)| node.live_parents == 0)
                .map(|(x, _)| (*x).to_string())
                .collect();

            let mut executor = get_executor()
                .await
                .map_err(|x| format!("Connection error: {}", x))?;

            while let Some(m) = nodes.pop() {
                println!("Executing {}", m);
                executor.execute(&m, asts.get(&m).unwrap()).await?;
                println!("Ready {}", m);
                println!("Graph {:?}", graph);

                let node = graph.get(m.as_str()).unwrap().clone();
                for n in node.next_nodes.iter() {
                    let mut node = graph.get_mut(n.as_str()).unwrap();
                    node.live_parents -= 1;
                    if node.live_parents == 0 {
                        nodes.push(n.to_string());
                    }
                }
            }
        }
        // Command::Docs => {
        //     let arrows: Vec<String> = dependencies
        //         .iter()
        //         .flat_map(|(x, y)| y.iter().map(move |z| format!("{z} -> {x}", x = x, z = z)))
        //         .collect();

        //     print!("{:?}", arrows);
        // }
        Command::Test { fail_fast } => {
            let mut exit_code = 0;
            let test_models = find_test_files(config.project.tests);
            let tests = load_tests(&test_models)?;
            let mut executor = get_executor().await?;

            for test in tests.iter() {
                match test {
                    Statement::Assert {
                        condition,
                        message: Some(message),
                    } => {
                        print!("{}", expr_to_message(message));

                        let query = format!("SELECT ({}) AS condition", condition);
                        let succeeded = executor.query_bool(&query).await?;

                        if succeeded {
                            println!("...OK")
                        } else {
                            if fail_fast {
                                std::process::exit(1);
                            }
                            exit_code = 1;

                            println!("...ERROR")
                        }
                    }
                    _ => unreachable!("Only Query & assert supported in tests"),
                }
            }
            std::process::exit(exit_code);
        }
    }
    Ok(())
}

#[cfg(test)]
#[macro_use]
extern crate maplit;

#[test]
fn test_dependencies() {
    let sql = "create materialized view x as select a from t";
    let ast = Parser::parse_sql(&PowerSqlDialect {}, sql).unwrap()[0].clone();

    let x = get_dependencies(&hashmap! {"x".to_string() => ast});

    assert_eq!(x, hashmap! {"x".to_string() => vec![]})
}

#[test]
fn test_dependencies_join() {
    let sql =
        "create materialized view x as select a from t join x on 1=1; create view t as select 1";
    let ast = Parser::parse_sql(&PowerSqlDialect {}, sql).unwrap();

    let x = get_dependencies(
        &hashmap! {"x".to_string() => ast[0].clone(), "t".to_string() => ast[1].clone()},
    );

    assert_eq!(
        x,
        hashmap! {"x".to_string() => vec!["t".to_string()], "t".to_string() => vec![]}
    );
}

#[test]
fn test_cycle_detection_err() {
    assert!(matches!(
        detect_cycles(&hashmap! {
            "a".to_string() => vec!["b".to_string()],
            "b".to_string() => vec!["a".to_string()]
        }),
        Err(_)
    ));
}

#[test]
fn test_cycle_detection_err_not_found() {
    assert!(matches!(
        detect_cycles(&hashmap! {
            "a".to_string() => vec!["b".to_string()],
            "b".to_string() => vec!["c".to_string()]
        }),
        Err(_)
    ));
}

#[test]
fn test_cycle_detection_ok() {
    assert!(matches!(
        detect_cycles(&hashmap! {
            "a".to_string() => vec!["b".to_string()],
            "b".to_string() => vec![]
        }),
        Ok(_)
    ));
}

#[test]
fn test_build_graph() {
    let deps = hashmap! {
        "a".to_string() => vec!["b".to_string()],
        "b".to_string() => vec!["c".to_string()],
        "c".to_string() => vec![],
    };
    let plan = build_graph(&deps).unwrap();

    assert_eq!(
        plan,
        hashmap! {
                "a" => ModelNode {
                    live_parents: 1,
                    next_nodes: vec![]
            },
                "b" => ModelNode {
                    live_parents: 1,
                    next_nodes: vec!["a".to_string()]
            },
                "c" => ModelNode {
                    live_parents: 0,
                    next_nodes: vec!["b".to_string()]
            }
        }
    );
}
