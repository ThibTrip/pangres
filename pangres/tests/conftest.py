#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import json
import logging
import os
import pandas as pd
from inspect import signature
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from typing import Optional, Dict

from pangres.helpers import _sqla_gt14, PandasSpecialEngine


# -

# # Helpers

class AutoDropTableContext:
    """
    This context manager drops given table (if exists) before giving back
    control and drops the table (if exists) again after you ran whichever tests
    you wanted.

    The context gets populated with every parameter of the init methods and
    if a DataFrame is provided, you get an instance of `pangres.helpers.PandasSpecialEngine`
    as well (under the attribute `pse`) which simplifies our testing.

    Examples
    --------
    >>> import pandas as pd
    >>> from pangres import upsert
    >>> from pangres.helpers import PandasSpecialEngine
    >>> from sqlalchemy import create_engine
    >>>
    >>> df = pd.DataFrame(index=pd.Index(data=[0], name='id'))
    >>> engine = create_engine('sqlite:///:memory:')
    >>> with AutoDropTableContext(engine=engine, df=df, schema=None, table_name='test') as ctx:
    ...     upsert(engine=engine, df=df, if_row_exists='update', schema=ctx.schema, table_name=ctx.table_name)
    ...     ctx.pse.table_exists()
    True
    >>> ctx.pse.table_exists()
    False
    """
    def __init__(self, engine:Engine, table_name:str, df:Optional[pd.DataFrame]=None,
                 schema:Optional[str]=None, dtype:Optional[Dict]=None, drop_on_exit:bool=True):
        if df is not None:
            self.pse = PandasSpecialEngine(engine=engine, df=df, table_name=table_name, schema=schema, dtype=dtype)
        schema = 'public' if 'postgres' in engine.dialect.dialect_description and schema is None else schema
        self.engine = engine
        self.schema = schema
        self.table_name = table_name
        self.namespace = f'{schema}.{table_name}' if schema is not None else table_name
        self.dtype = dtype
        self._drop_on_exit = drop_on_exit

    # helper
    def drop_table(self):
        with self.engine.connect() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS {self.namespace};'))
            if hasattr(connection, 'commit'):
                connection.commit()

    def __enter__(self):
        self.drop_table()
        return self

    def __exit__(self, type, value, traceback):
        if self._drop_on_exit:
            self.drop_table()
        return False  # raise any error


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
