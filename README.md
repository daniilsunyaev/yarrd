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
- allow store tables into files (may need to break this down) (straight bincode serialization)
  - ✓ add serializer/deserializer for each type (or use bincode)
  - ✗ implement custom serializer for rows
  - ✓ change tables to store rows in bytes, and deserialize it on-demand
  - ✓ add serialization/deserialization error and return it in serialize module
  - ✓ add null bitmask
  - store tables rows on close
  - load tables on db connect
- fix null ints not allowed
- introduce query result struct instead of Row vec
- think of extracting table row to separate class to incapsulate offset operations
- introduce page alignment
- add is Null check
- remove result from where closures, cmp should return false in case of undefined, or think of three-valued logic
- float values
- allow capsed keywords
- extract table name, column name parsing to a method
- implement primary constraint (may just primary key flag, no general constraints)
- add row_id
- store hashtable for primary keys at the beginning of file or store those in root database file
- maybe use peek and rewrite parser in more of decoupeled manner?
- add table column names -> column offset hashmap
- add pretty output of queries
- implement limit
- implement joins
- WAL
- restore from journal
- transactions
