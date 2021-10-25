#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import pandas as pd
import json
from sqlalchemy import create_engine, text

from pangres.helpers import _sqla_gt14


# -

# # Helpers

def drop_table_if_exists(engine, schema, table_name):
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    with engine.connect() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS {namespace};'))
        if hasattr(connection, 'commit'):
            connection.commit()


# ## Class TestDB

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
