#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import json
import pandas as pd
import sqlalchemy as sa
from functools import wraps
from inspect import signature
from sqlalchemy import create_engine, text
from typing import Union

from pangres.helpers import _sqla_gt14


# -

# # Helpers for other test modules

# +
def _get_function_param_value(sig, param_name, args, kwargs):
    """
    Helper to retrieve the value of a specific parameter
    from *args and **kwargs like when we wrap a function
    """
    param_ix = list(sig.parameters).index(param_name)
    if param_ix <= len(args) - 1:
        return args[param_ix]
    else:
        return kwargs[param_name]


def table_exists(connection, schema, table_name) -> bool:
    insp = sa.inspect(connection)
    if _sqla_gt14():
        return insp.has_table(schema=schema, table_name=table_name)
    else:
        return table_name in insp.get_table_names(schema=schema)


def select_table(engine, schema, table_name,
                 error_if_missing=True,
                 **read_sql_kwargs) -> Union[pd.DataFrame, None]:
    """
    Does a simple SELECT * FROM {table} and returns a DataFrame from that.
    Has an option to return None if the table does not exist and
    `error_if_missing` is False.
    """
    ns = f'{schema}.{table_name}' if schema is not None else table_name
    with engine.connect() as con:
        # check if the table is present
        if table_exists(connection=con, schema=schema, table_name=table_name):
            return pd.read_sql(text(f'SELECT * FROM {ns}'), con=con, **read_sql_kwargs)
        elif error_if_missing:
            raise AssertionError(f'Table {ns} does not exist')
        else:
            return None


def drop_table(engine, schema, table_name):
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    with engine.connect() as connection:
        # instead of using `DROP TABLE IF EXISTS` which produces warnings
        # in MySQL when the table does exist, we will just check if it does exist
        # and using a `DROP TABLE` query accordingly
        if not table_exists(connection=connection, schema=schema,
                            table_name=table_name):
            return
        # make sure to commit this!
        connection.execute(text(f'DROP TABLE {namespace};'))
        if hasattr(connection, 'commit'):
            connection.commit()


# thanks to https://stackoverflow.com/a/42581103
def drop_table_for_test(table_name, drop_before_test=True, drop_after_test=True):
    """
    Decorator for wrapping our tests functions. Expects the tests function
    to have the arguments "engine" and "schema" and will retrieve their values
    for dropping the table named `table_name`.

    Parameters
    ----------
    drop_before_test
        Whether to drop the table before executing the test
    drop_after_test
        Whether to drop the table after executing the test
    """
    assert drop_before_test or drop_after_test, 'One of drop_before_test or drop_after_test must be True'

    def sub_decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            # get engine and schema
            sig = signature(function)
            assert 'engine' in sig.parameters
            assert 'schema' in sig.parameters
            engine = _get_function_param_value(sig=sig, param_name='engine', args=args, kwargs=kwargs)
            schema = _get_function_param_value(sig=sig, param_name='schema', args=args, kwargs=kwargs)

            # before test
            if drop_before_test:
                drop_table(engine=engine, schema=schema, table_name=table_name)

            # test
            function(*args, **kwargs)

            # after test
            if drop_after_test:
                drop_table(engine=engine, schema=schema, table_name=table_name)
            return
        return wrapper
    return sub_decorator


def get_table_namespace(schema, table_name):
    return f'{schema}.{table_name}' if schema is not None else table_name


def commit(connection):
    if hasattr(connection, 'commit'):
        connection.commit()


class TableNames:
    ADD_NEW_COLUMN = 'test_add_new_column'
    BAD_COLUMN_NAMES = 'test_bad_column_names'
    BAD_TEXT = 'test_bad_text'
    CHANGE_EMPTY_COL_TYPE = 'test_change_empty_col_type'
    COLUMN_NAMED_VALUES = 'test_column_named_values'
    COMMIT_AS_YOU_GO = 'test_commit_as_you_go'
    COMMIT_OR_ROLLBACK_TRANS = 'test_commit_or_rollback_trans'
    CREATE_SCHEMA_NONE = 'test_create_schema_none'
    CREATE_SCHEMA_NOT_NONE = 'test_create_schema_not_none'
    END_TO_END = 'test_end_to_end'
    INDEX_ONLY_INSERT = 'test_index_only_insert'
    INDEX_WITH_NULL = 'test_index_with_null'
    MULTIINDEX = 'test_multiindex'
    REUSE_CONNECTION = 'test_reuse_connection'
    TABLE_CREATION = 'test_table_creation'
    UNIQUE_KEY = 'test_unique_key'
    VARIOUS_CHUNKSIZES = 'test_chunksize'
    WITH_YIELD = 'test_with_yield'
    WITH_YIELD_EMPTY = 'test_with_yield_empty'
    # case when we need a table name but we are not going to use it for
    # creating/updating a table (if you see it in the DB, something went wrong)
    NO_TABLE = 'test_no_table'


# name of the PostgreSQL schema used for testing schema creation
schema_for_testing_creation = 'pangres_create_schema_test'


# -

# ## Class TestDB

# +
def pytest_addoption(parser):
    parser.addoption('--sqlite_conn', action="store", type=str, default=None)
    parser.addoption('--pg_conn', action="store", type=str, default=None)
    parser.addoption('--mysql_conn', action="store", type=str, default=None)
    parser.addoption('--pg_schema', action='store', type=str, default=None)


def pytest_generate_tests(metafunc):
    # this is called for every test
    # if we see the parameters "engine" and "schema" in a function
    # then we will repeat the test for each engine
    func_params = signature(metafunc.function).parameters
    if not ('engine' in func_params and 'schema' in func_params):
        # I could not find any other way than to add a dummy
        # for executing a test only once (parameterize needs arguments)
        metafunc.parametrize('_', [''], scope='module')
        return

    # tests that we need to repeat for each engine + options (e.g. future)
    conn_strings = {'sqlite':metafunc.config.option.sqlite_conn,
                    'pg':metafunc.config.option.pg_conn,
                    'mysql':metafunc.config.option.mysql_conn}
    engines, schemas, ids = [], [], []
    for db_type, conn_string in conn_strings.items():
        if conn_string is None:
            continue
        schema = metafunc.config.option.pg_schema if db_type == 'pg' else None
        engine = create_engine(conn_string)
        schemas.append(schema)
        engines.append(engine)
        schema_id = '' if schema is None else f'_schema:{schema}'
        ids.append(f'{engine.url.drivername}{schema_id}')
        # for sqlalchemy 1.4+ use future=True to try the future sqlalchemy 2.0
        if _sqla_gt14():
            future_engine = create_engine(conn_string, future=True)
            schemas.append(schema)
            engines.append(future_engine)
            ids.append(f'{engine.url.drivername}{schema_id}_future')
    assert len(engines) == len(schemas) == len(ids)
    if len(engines) == 0:
        raise ValueError('You must provide at least one connection string (e.g. argument --sqlite_conn)!')
    metafunc.parametrize("engine, schema", list(zip(engines, schemas)), ids=ids, scope='module')


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
