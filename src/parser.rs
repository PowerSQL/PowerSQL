#[derive(Debug)]
pub struct PowerSqlDialect {}

impl sqlparser::dialect::Dialect for PowerSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        // ANSI SQL
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '_'
    }
}
