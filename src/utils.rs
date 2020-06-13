pub fn base_name(file_name: &str) -> &str {
    return file_name
        .trim_end_matches(".sql")
        .split('/')
        .last()
        .unwrap();
}
