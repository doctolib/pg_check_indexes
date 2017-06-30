# pg_check_indexes
Test your PostgreSQL btree indexes for logical corruption

## How to
To use this program you should have:
- Ruby 2.4.0
- Gem pg
- Gem optiparse

To use it, navigate to the directory you've downloaded and issue:
```ruby pg_check_indexes.rb``` 

It will test all the indexes in the database that you connect by default as if you're using psql (usually the same as your username in a local socket connection to PostgreSQL).

You have some more advanced options for other databases or servers:
```
    -d, --database DBNAME            The database name to connect to
    -u, --user DBUSER                PostgreSQL user name
    -h, --hostname DBHOSTNAME        PostgreSQL hostname
    -p, --port DBPORT                PostgreSQL listen port
    -t, --table TABLENAME            Check only a selected table, use schema qualified names.
    -T, --threshold MAX_TABLE_SIZE   in bytes, 0 for unlimited
    -s, --stop-on-failure            If "y", execution will stop when corruption found
```

It's recommended that you set a threshold of some megabytes for a first run, specially if you're testing large tables and indexes, the program will skip tables larger then the specified value.

The password should be entered manually if required by your pg_hba.conf settings, or you can use .pgpass
Since the Gem pg uses libpq, this program behaves as if you're using standard PostgreSQL tools.

The flag `--stop-on-failure` may be useful in case you want to stop testing when a first corruption is found.

The program will exit with 0 if no corruption is found otherwise it will exit with 1, so it's easily integrated with deployment automation or continuous integration tools like Jenkins.

## Credits and License
This program was developed by [Doctolib](https://www.doctolib.fr).

It's licensed under the standard MIT license.
