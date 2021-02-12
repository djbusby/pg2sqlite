# Convert PostgreSQL to Sqlite

It converts a PostgreSQL database to Sqlite.

* Tables: Works! and (mostly) maps data types
* Foreign Keys: Sorta
* Views: Not Yet, is it needed?
* Materialized View: Not Likely
* Functions: Nope


```shell
./pg2sqlite.php \
  --source=pgsql:DSN \
  --output=sqlite:DSN \
  --filter=/regular-expression/
```

The **source** parameter is required.

The **output** parameter is optional, the tool generates a unique-ish name.

The **filter** parameter is optional.
If provided it becomes a pass-filter for the tables to export.
The `/` delimiters are required.


## Filter Examples

The filter can be any regular expression.
Whatever parameter you provide is passed directly to `preg_match`.


```
# all tables with "contact" in the name
--filter=/contact/

# only the "contact" table
--filter=/^contact$/

# only "table_a" and "table_b"
--filter=/^(table_a|table_b)$/

# starts with "contact" or is "table_a" or "table_b"
--filter=/^(contact.*|(table_a|table_b))$/
```


## See Also

* https://stackoverflow.com/questions/6148421/how-to-convert-a-postgres-database-to-sqlite
* https://github.com/caiiiycuk/postgresql-to-sqlite
* https://www2.sqlite.org/cvstrac/wiki?p=ConverterTools
