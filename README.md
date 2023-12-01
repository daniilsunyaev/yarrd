# YARRD

stands for Yet Another Row-oriented Relational Database

General assumptions:
- row oriented
- relational (well, it is not yet)
- single threaded
- sql-like syntax, no subqueries
- no external dependencies

## Quickstart

```
cargo run

yarrd> .createdb database

yarrd> .connect database

yarrd> create table users (id int, name string)

yarrd> insert into users (name, id) values ("john", 3)

yarrd> insert into users (id, name) values (1, NULL)

yarrd> select id from users where name = "john"
```

result is:

`Some(QueryResult { column_types: [Integer], column_names: ["id"], rows: [Row { bytes: [254, 3, 0, 0, 0, 0, 0, 0, 0] }] })`

the important part is

`bytes: [254, 3, 0, 0, 0, 0, 0, 0, 0]`

`254` is `11111110`, a null bitmask which means that first column (id) is not null.

`[3, 0, 0, 0, 0, 0, 0, 0]` is actual column bytes. Integers are stored as 8 byte, least
significant first. So you have your `3` id.

```
yarrd> select name from users where id = 3

yarrd> Some(QueryResult { column_types: [String], column_names: ["name"], rows: [Row { bytes: [254, 4, 106, 111, 104, 110, 0, 0, ...] }] })
```

here, we have these bytes:

```
[254, 4, 106, 111, 104, 110, 0, 0, ...]
```

`254` is null bitmask,

`4` is the string length,

`106, 111, 104, 110` is `j`, `o`, `h`, `n`.

All strings stored with fixed 256 bytes alignment, that's why there is bunch of zeroes.

## Commands Reference

### Metacommands

`.createdb DATABASE_PATH [DATABASE_TABLES_DIR_PATH]`

Create a new database at `DATABASE_PATH`. Path can be relative or absolute.
No file should exist at given path.
If `DATABASE_TABLES_DIR_PATH` is given, it will place all tables' files into that dir.
It will create this dir if it is not existing yet. If no path specified, it will use
database name + `_tables` suffix in current folder as a tables dir.

`.createdb test_app`

`.createdb ~/dev/some_app/dev_app ~/dev/some_app/dev_app/tables`

---

`.dropdb DATABASE_PATH`

Remove database at specified path. Tables files will be cleaned out as well,
but tables dir won't be removed.
This metacommand can only be executed if no database is currently connected.

`.dropdb test_app`

---

`.connect DATABASE_PATH`

Establish connection to database at specified path. Path can be absolute or reative.
Once executed, all sql statements will be executed on this database.

`.connect dev_app`

`.connect /home/user/tmp/database.db`

---

`.close`

Close database connection. All unflushed changes will be recorded to disk.

---

`.exit` or `.quit`

Close database connection and exit from cmd interface.

## Query commands

Querying syntax is similar to sql, but have no semicolon at the end.

Supported statemes: `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `SELECT`, `UPDATE`, `DELETE FROM`, `ALTER TABLE`, `VACUUM`.
Supported constraints: `NOT NULL`, `DEFAULT`.

`CREATE TABLE users (id INT NOT NULL, name STRING, age INT NOT NULL)`

`insert into users (name, id) values ("John", 2)`

`SELECT *, id FROM users WHERE id > 5`

`update users set name="John Doe" where name is null`

`DELETE FROM users WHERE id = 2`

`alter table users add rating float`

`ALTER TABLE users ADD CONSTRAINT DEFAULT 20 (age)`

`ALTER TABLE users DROP CONSTRAINT NOT NULL (age)`

`vacuum`

`Drop table users`

## Checklist

- ✓ add prompt
- ✓ add basic lexer
- ✓ add basic parser
- ✓ add create/drop table parsing
- ✓ allow strings, limit identificators to non-whitespaced chars
- ✗ allow 'int' name for columns?
- ✓ exit metacommand
- ✓ parse insert into
- ✓ parse select (without conditions)
- ✓ parse where
- ✓ parse update
- ✓ parse delete
- ✓ execute create table
- ✓ execute drop table
- ✓ execute select
- ✓ execute select where
- ✓ execute insert
- ✓ execute update
- ✓ execute delete (in-memory)
- ✓ reuse deleted rows
- ✓ think of proper error handling
- ✓ allow store tables into files (may need to break this down) (straight bincode serialization)
  - ✓ add serializer/deserializer for each type (or use bincode)
  - ✗ implement custom serializer for rows
  - ✓ change tables to store rows in bytes, and deserialize it on-demand
  - ✓ add serialization/deserialization error and return it in serialize module
  - ✓ add null bitmask
  - ✓ store tables rows on close
  - ✓ load tables on db connect
- ✓ fix null ints not allowed
- ✓ introduce query result struct instead of Row vec
- ✓ think of extracting table row to separate class to incapsulate offset operations
- ✓ introduce page alignment (may need to break down)
  - ✓ create simple lru storage
  - ✓ read page from disk, flush page
  - ✓ use pager in table for read/write operations
  - ✓ track max rows to avoid getting deleted rows at the end of last page
  - ✓ add flushed flag to page
- ✓ use tempfile dir in command specs
- ✓ extract page to separate file
- ✓ add is Null check
- ✓ remove result from where closures, cmp should return false in case of undefined, or think of three-valued logic
- ✓ refactor cmp_operator a bit
- ✓ add hard limit to row size
- ✓ float values
- ✗ think of bitwise version of cmp operator
- ✓ think of table error or table init error
- ✓ allow capsed keywords
- ✓ extract table name, column name parsing to a method
- ✓ alter table parsing
  - ✓ rename table
  - ✓ rename column
  - ✓ add column
  - ✓ drop column
- ✓ alter table execution
  - ✓ rename table
  - ✓ rename column
  - ✓ add column
  - ✓ drop column
- ✓ implement vacuum metacommand or something like that
- ✓ add .create/.drop metacommand
- ✓ add .connect/.close metacommands
- ✓ add metacommands docs
- ✗ add row_id to service bytes
- ✓ implement not null constraint
- ✓ allow to update constraints
- ✓ implement default constraint
- ✓ implement check constraint
- ✓ `SELECT id FROM users WHERE "users.name" = name` should not return all records
- implement create index
  - ✓ store hashtable for indexed keys at the hash file and allow to search through index
  - ✓ introduce overflow pages (handle multiple equal values)
  - ✓ increase index buckets count if rows / total hash space > 0.5
  - ✓ update hashtable on insert
  - ✓ update hashtable on delete
  - ✓ update hashtable on update
  - ✓ update hashtable on vacuum
  - ✓ allow to create index on table, save index in schema
  - ✓ allow to drop index on table and drop indexes on drop table
  - adjust index on alter table (rename table, rename column, drop column, add column)
  - implement REINDEX
- do not allow two columns with the same names in a table
- implement unique constraint
- introduce AND and allow WHERE to accept multiple conditions
- implement primary constraint and use row_id if not set
- think if we should rename 'validate_row_over_constraint' to smth like "check_not_null_constraints"
- check if we can avoid generating byte layout for every row when using where
- introduce NOT
- dry parser 'parse_index_name', 'parse_column_name' etc, since those differ only be error messages
- remove tables dir if it is empty after tables cleanup
- maybe use peek and rewrite parser in more of decoupeled manner? Try to allow keyword names
- add table column names -> column offset hashmap
- add pretty output of queries
- implement limit
- `insert into users (id) values (1,2)` should not crash but show an error instead
- think if we can handle multipage rows or maybe should make page size dynamic
- implement joins
- fix result_large_err clippy warnings
- pressing up should restore previous command
- make table recreation incremental (insead of full select from old table)
- current command should be editable (left and right should reposition input cursor)
- subconditions (AND with combinations)
- allow to store strings with top limit less than 255 symbols (which will take less space)
- think of calculating row cell offset via null bitmask, so null cells won't occupy space on disk
- WAL
- think of adding "cascade" file manager to easily rollback changes if failed on some step
- handle errors on db close and flush
- restore from journal
- support non-ascii chars
- transactions
- AST
- think of implementing btree index
- think of metalexer
