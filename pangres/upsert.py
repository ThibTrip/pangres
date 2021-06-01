"""
Functions for preparing/compiling and executing upsert statements
in different SQL flavors.
"""
from copy import deepcopy
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql.dml import insert as mysql_insert

# # Postgres

def postgres_upsert(engine, table, values, if_row_exists):
    """
    Prepares and executes a PostgreSQL INSERT ON CONFLICT...DO NOTHING
    or DO UPDATE statement via sqlalchemy.dialects.postgresql.insert

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
    table : sqlalchemy.sql.schema.Table
    values : list of dict
    if_row_exists : {'update', 'ignore'}
        * If 'update' issues a ON CONFLICT...DO UPDATE statement
        * If 'ignore' issues a ON CONFLICT...DO NOTHING statement
    """
    insert_stmt = pg_insert(table).values(values)
    if if_row_exists == 'update':
        update_cols = [c.name
                       for c in table.c
                       if c not in list(table.primary_key.columns)]
        # case when there is only an index in the DataFrame i.e. no columns to update
        if len(update_cols) == 0:
            if_row_exists = 'ignore'
        else:
            upsert = insert_stmt.on_conflict_do_update(index_elements=table.primary_key.columns,
                                                       set_={k: getattr(insert_stmt.excluded, k)
                                                             for k in update_cols})
    if if_row_exists == 'ignore':
        upsert = insert_stmt.on_conflict_do_nothing()
    # execute upsert
    with engine.connect() as connection:
        return connection.execute(upsert)

# # MySQL

def mysql_upsert(engine, table, values, if_row_exists):
    """
    Prepares and executes a MySQL INSERT IGNORE or
    INSERT...ON DUPLICATE KEY UPDATE
    via sqlalchemy.dialects.mysql.dml.insert

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
    table : sqlalchemy.sql.schema.Table
    values : list of dict
    if_row_exists : {'update', 'ignore'}
        * If 'update' issues a ON DUPLICATE KEY UPDATE statement
        * If 'ignore' issues a INSERT IGNORE statement

    Examples
    --------
    >>> import datetime
    >>> from sqlalchemy import create_engine, VARCHAR
    >>> from pangres.examples import _TestsExampleTable
    >>> from pangres.helpers import PandasSpecialEngine
    >>> 
    >>> engine = create_engine('mysql+pymysql://username:password@localhost:3306/db')  # doctest: +SKIP
    >>> df = _TestsExampleTable.create_example_df(nb_rows=5)
    >>> df # doctest: +SKIP
    | profileid   | email             | timestamp                 |   size_in_meters | likes_pizza   | favorite_colors              |
    |:------------|:------------------|:--------------------------|-----------------:|:--------------|:-----------------------------|
    | abc0        | foobaz@gmail.com  | 2007-10-11 23:15:06+00:00 |          1.93994 | False         | ['yellow', 'blue']           |
    | abc1        | foobar@yahoo.com  | 2007-11-21 07:18:20+00:00 |          1.98637 | True          | ['blue', 'pink']             |
    | abc2        | foobaz@outlook.fr | 2002-09-30 17:55:09+00:00 |          1.55945 | True          | ['blue']                     |
    | abc3        | abc@yahoo.fr      | 2007-06-13 22:08:36+00:00 |          2.2495  | True          | ['orange', 'blue']           |
    | abc4        | baz@yahoo.com     | 2004-11-22 04:54:09+00:00 |          2.2019  | False         | ['orange', 'yellow', 'blue'] |

    >>> pse = PandasSpecialEngine(engine=engine, df=df, table_name='test_upsert_mysql') # doctest: +SKIP
    >>> 
    >>> insert_values = {'profileid':'abc5', 'email': 'toto@gmail.com',
    ...                  'timestamp': datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
    ...                  'size_in_meters':1.9,
    ...                  'likes_pizza':True,
    ...                  'favorite_colors':['red', 'pink']}
    >>> 
    >>> df.head(0).to_sql('test_upsert_mysql', con=engine, if_exists='replace', dtype={'profileid':VARCHAR(10)}) # doctest: +SKIP
    >>> mysql_upsert(engine=engine, table=pse.table,
    ...              values=list(insert_values.values()), if_row_exists='update') # doctest: +SKIP
    """
    insert_stmt = mysql_insert(table).values(values)
    if if_row_exists == 'update':
        # thanks to: https://stackoverflow.com/a/58180407/10551772
        # prepare kwargs for on_duplicated_key_update (with kwargs and getattr
        # even "bad" column names will resolve e.g. columns with spaces)
        update_cols = {}
        for col in insert_stmt.table.columns:
            col_name = col.name
            if col_name not in table.primary_key:
                update_cols.update({col_name:getattr(insert_stmt.inserted, col_name)})
        # case when there is only an index in the DataFrame i.e. no columns to update
        if len(update_cols) == 0:
            if_row_exists = 'ignore'
        else:
            upsert = insert_stmt.on_duplicate_key_update(**update_cols)
    if if_row_exists == 'ignore':
        # thanks to: https://stackoverflow.com/a/50870348/10551772
        upsert = insert_stmt.prefix_with('IGNORE')

    # execute upsert
    with engine.connect() as connection:
        return connection.execute(upsert)

# # Sqlite 
# (or other databases where ON CONFLICT...DO UPDATE SET/DO NOTHING is supported)

def sqlite_upsert(engine, table, values, if_row_exists):
    """
    Compiles and executes a SQlite ON CONFLICT...DO NOTHING or DO UPDATE
    statement.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
    table : sqlalchemy.sql.schema.Table
    values : list of dict
    if_row_exists : {'update', 'ignore'}
        * If 'update' issues a ON CONFLICT...DO UPDATE statement
        * If 'ignore' issues a ON CONFLICT...DO NOTHING statement

    Examples
    --------
    >>> import datetime
    >>> from sqlalchemy import create_engine
    >>> from pangres.examples import _TestsExampleTable
    >>> from pangres.helpers import PandasSpecialEngine
    >>> 
    >>> engine = create_engine('sqlite:///:memory:')
    >>> df = _TestsExampleTable.create_example_df(nb_rows=5)
    >>> df # doctest: +SKIP
    | profileid   | email             | timestamp                 |   size_in_meters | likes_pizza   | favorite_colors              |
    |:------------|:------------------|:--------------------------|-----------------:|:--------------|:-----------------------------|
    | abc0        | foobaz@gmail.com  | 2007-10-11 23:15:06+00:00 |          1.93994 | False         | ['yellow', 'blue']           |
    | abc1        | foobar@yahoo.com  | 2007-11-21 07:18:20+00:00 |          1.98637 | True          | ['blue', 'pink']             |
    | abc2        | foobaz@outlook.fr | 2002-09-30 17:55:09+00:00 |          1.55945 | True          | ['blue']                     |
    | abc3        | abc@yahoo.fr      | 2007-06-13 22:08:36+00:00 |          2.2495  | True          | ['orange', 'blue']           |
    | abc4        | baz@yahoo.com     | 2004-11-22 04:54:09+00:00 |          2.2019  | False         | ['orange', 'yellow', 'blue'] |

    >>> pse = PandasSpecialEngine(engine=engine, df=df, table_name='test_upsert_sqlite')
    >>> 
    >>> insert_values = {'profileid':'abc5', 'email': 'toto@gmail.com',
    ...                  'timestamp': datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
    ...                  'size_in_meters':1.9,
    ...                  'likes_pizza':True,
    ...                  'favorite_colors':['red', 'pink']}
    >>> 
    >>> sqlite_upsert(engine=engine, table=pse.table,
    ...               values=list(insert_values.values()), if_row_exists='update') # doctest: +SKIP
    """
    def escape_col(col):
        # unbound column from its table
        # otherwise the column would compile as "table.col_name"
        # which we could not use in e.g. SQlite
        unbound_col = deepcopy(col)
        unbound_col.table = None
        return str(unbound_col.compile(dialect=engine.dialect))

    # prepare start of insert (INSERT VALUES (...) ON CONFLICT)
    insert = SQLCompiler(dialect=engine.dialect,
                         statement=table.insert().values(values))

    # append on conflict clause
    pk = [escape_col(c) for c in table.primary_key]
    non_pks = [escape_col(c) for c in table.columns if c not in list(table.primary_key)]
    ondup = f'ON CONFLICT ({",".join(pk)})'
    # always use "DO NOTHING" if there are no primary keys
    if (not non_pks) or (if_row_exists == 'ignore'):
        ondup_action = 'DO NOTHING'
        insert.string = ' '.join((insert.string, ondup, ondup_action))
    elif if_row_exists == 'update':
        ondup_action = 'DO UPDATE SET'
        updates = ', '.join(f'{c}=EXCLUDED.{c}' for c in non_pks)
        insert.string = ' '.join((insert.string, ondup, ondup_action, updates))
    with engine.connect() as connection:
        return connection.execute(insert)
