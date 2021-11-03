#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import pandas as pd
import json
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy import inspect as sqla_inspect

from pangres.helpers import _sqla_gt14


# -

# # Helpers

# ## Functions for dropping a table

# +
def _drop_table_from_conn(connection, schema, table_name):
    """
    SQL injection safe table dropping. Does not raise if table
    does not exist.
    Also works in all (?) versions of sqlalchemy.

    Notes
    -----
    Reflecting to get the table model and then using drop or drop_all
    is the only way I found for dropping tables in async context
    (raw SQL did not work and neither did sqlalchemy.schema.DropTable).
    See coroutine adrop_table_if_exists.
    """
    meta = MetaData(bind=connection, schema=schema)
    table = Table(table_name, meta)
    insp = sqla_inspect(connection)
    # try to drop, if it fails check if it is due to table absence
    # this is faster
    try:
        if hasattr(insp, 'reflect_table'):
            insp.reflect_table(table, include_columns=None)
        else:
            meta.reflect(only=[table_name])
        meta.drop_all()
    except Exception as e:
        insp = sqla_inspect(connection)
        if hasattr(insp, 'has_table'):
            has_table = insp.has_table(schema=schema, table_name=table_name)
        else:
            has_table = connection.dialect.has_table(connection=connection, schema=schema,
                                                     table_name=table_name)
        if has_table:
            raise e


def drop_table_if_exists(engine, schema, table_name):
    with engine.connect() as connection:
        _drop_table_from_conn(connection=connection, schema=schema, table_name=table_name)
        if hasattr(connection, 'commit'):
            connection.commit()


async def adrop_table_if_exists(engine, schema, table_name):
    async with engine.connect() as connection:
        await connection.run_sync(_drop_table_from_conn, schema=schema, table_name=table_name)
        await connection.commit()

# +
def pytest_addoption(parser):
    parser.addoption('--sqlite_conn', action="store", default=None)
    parser.addoption('--pg_conn', action="store", default=None)
    parser.addoption('--mysql_conn', action="store", default=None)
    parser.addoption('--pg_schema', action='store', default=None)

def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    conn_strings = {'sqlite':metafunc.config.option.sqlite_conn,
                    'pg':metafunc.config.option.pg_conn,
                    'mysql':metafunc.config.option.mysql_conn}
    engines = []
    schemas = []
    for db_type, conn_string in conn_strings.items():
        if conn_string is None:
            continue
        schema = metafunc.config.option.pg_schema if db_type == 'pg' else None
        engine = create_engine(conn_string)
        schemas.append(schema)
        engines.append(engine)
        # for sqlalchemy 1.4+ use future=True to try the future sqlalchemy 2.0
        if _sqla_gt14():
            future_engine = create_engine(conn_string, future=True)
            schemas.append(schema)
            engines.append(future_engine)
    assert len(engines) == len(schemas)
    if len(engines) == 0:
        raise ValueError('You must provide at least one connection string (e.g. argument --sqlite_conn)!')
    metafunc.parametrize("engine, schema", list(zip(engines, schemas)), scope='module')


# -

# ## Function to read back from database data we inserted
# We need to apply a few modification for comparing DataFrames we get back from the DB and DataFrames we expect e.g. for JSON (with SQlite pandas reads it as string).

def read_example_table_from_db(engine, schema, table_name):
    def load_json_if_needed(obj):
        """
        For SQlite we receive strings back (or None) for a JSON column.
        For Postgres we receive lists or dicts (or None) back.
        """
        if isinstance(obj, str):
            return json.loads(obj)
        return obj
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    with engine.connect() as connection:
        df_db = (pd.read_sql(text(f'SELECT * FROM {namespace}'), con=connection, index_col='profileid')
                 .astype({'likes_pizza':bool})
                 .assign(timestamp=lambda df: pd.to_datetime(df['timestamp'], utc=True))
                 .assign(favorite_colors= lambda df: df['favorite_colors'].map(load_json_if_needed)))
    return df_db
