#!/usr/bin/env python
# coding: utf-8
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import pandas as pd
import json
from sqlalchemy import create_engine

# # Helpers

def drop_table_if_exists(engine, schema, table_name):
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    engine.execute(f'DROP TABLE IF EXISTS {namespace};')


# ## Class TestDB

# +
def pytest_addoption(parser):
    parser.addoption('--conn_string', action="store")
    parser.addoption('--schema', action='store', default=None)


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    conn_string = metafunc.config.option.conn_string
    engine = create_engine(conn_string)
    schema = metafunc.config.option.schema
    metafunc.parametrize("engine", [engine])
    metafunc.parametrize("schema", [schema])


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
    df_db = (pd.read_sql(f'SELECT * FROM {namespace}', con=engine, index_col='profileid')
             .astype({'likes_pizza':bool})
             .assign(timestamp=lambda df: pd.to_datetime(df['timestamp'], utc=True))
             .assign(favorite_colors= lambda df: df['favorite_colors'].map(load_json_if_needed)))
    return df_db
