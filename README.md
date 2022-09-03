# YARRD

stands for Yet Another Row-oriented Relational Database

General assumptions:
- row oriented
- relational (well, it is not yet)
- single threaded
- sql-like syntax, no subqueries
- no external dependencies

### Quickstart

```
cargo run

yarrd> .createdb database

yarrd> .connect database

yarrd> create table users (id int, name string)

yarrd> insert into users (name, id) values ("john", 3)

yarrd> insert into users (id, name) values (1, NULL)

yarrd> select id from users where name = john
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

### Checklist
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
- add metacommands docs
- implement primary constraint (may be just a primary key flag, no general constraints)
- add row_id
- introduce NOT
- remove tables dir if it is empty after tables cleanup
- store hashtable for primary keys at the beginning of file or store those in root database file
- maybe use peek and rewrite parser in more of decoupeled manner? Try to allow keyword names
- add table column names -> column offset hashmap
- add pretty output of queries
- implement limit
- `insert into users (id) values (1,2)` should not crash but show an error instead
- implement joins
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
- think of metalexer
