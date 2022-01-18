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
- parse update
- parse delete
- execute create table
- execute drop table
- execute select
- execute update
- execute delete (in-memory)
- think of proper error handling
- float values
- allow store tables into files (may need to break this down) (straight bincode serialization)
- load stored tables on launch
- implement primary constraint (may just primary key flag, no general constraints)
- implement custom serializer for rows
- introduce page alignment
- store hashtable for primary keys at the beginning of file or store those in root database file
- implement limit
- implement joins
- WAL
- restore from journal
- transactions
