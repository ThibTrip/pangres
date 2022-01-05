#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import json
import pandas as pd
from functools import wraps
from inspect import signature
from sqlalchemy import create_engine, text

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


def drop_table(engine, schema, table_name):
    # make sure to commit this!
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    with engine.connect() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS {namespace};'))
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
        ids.append(f'{engine.url.drivername}_{schema}')
        # for sqlalchemy 1.4+ use future=True to try the future sqlalchemy 2.0
        if _sqla_gt14():
            future_engine = create_engine(conn_string, future=True)
            schemas.append(schema)
            engines.append(future_engine)
            ids.append(f'{engine.url.drivername}_{schema}_future')
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
