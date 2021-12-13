Testing how pangres could work with a single transaction and connection for all operations.

# Files

* pangres_imitation.py: mock of pangres
* test_end_to_end.py: tests for pytest

# Testing

Install pangres. After that install loguru with `pip install loguru`.

The arguments of the command below are the same as for testing pangres (except the postgres schema which I have omitted because it's more work).
You have to be in the same folder where the tests are for this command to work.

```
pytest -xv . --sqlite_conn=$SQLITE_CONNECTION_STRING --pg_conn=$POSTGRES_CONNECTION_STRING --mysql_conn=$MYSQL_CONNECTION_STRING
```