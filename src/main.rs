use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use toml::Value;
use walkdir::{Error, WalkDir};

pub fn main() -> Result<(), Error> {
    let value = "foo = 'bar'".parse::<Value>().unwrap();
    print!("{:?}", value);

    let dialect = AnsiDialect {};

    for entry in WalkDir::new("examples") {
        let entry = entry?;
        if let Some(abc) = entry.path().extension() {
            {
                if abc == "sql" {
                    print!("{:?}", entry.path());
                }
            }
        }
    }

    let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

    let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();

    println!("AST: {:?}", ast);

    println!("Running");

    return Ok(());
}
