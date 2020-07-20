## [Unreleased]

## [0.3.0] - 2020-07-20

### Added

- Added `ASSERT`-based testing. This makes it easy to write easy & complex tests using SQL conditions.
- Add `--fail-fast` option to `powersql test`. In this case, tests will fail immediately after the first error.
- Various extensions to reference finder, so it will find the used tables in queries more often.

### Removed

- Testing with queries that fail on non-zero rows. For consistency, every test now can be expressed using `ASSERT`.

## [0.2.0] - 2020-07-14

### Added

- Added BigQuery support 🎉
- Added a `CHANGELOG.md` file

### Changed

- Reduced number of dependencies for PostgreSQL executor

### Internal

- Made internal changes to support multiple backends
