#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import asyncio
import importlib
import inspect
import json
import pandas as pd
import sqlalchemy as sa
from contextlib import contextmanager
from functools import wraps
from inspect import signature
from sqlalchemy import create_engine, text
#from sqlalchemy.engine import URL
from sqlalchemy.engine.url import URL
from typing import Optional, Union

# local imports
from pangres import aupsert, upsert
from pangres.helpers import _sqla_gt14
# -

# # Helpers for other test modules

# ## Async/Sync detection and handling

# dict where the keys are patterns to find async drivers
# (not regex patterns we'll just use the Python `in` operator)
# and the values are sync dialect names for sqlalchemy
# this will allow us to detect connection strings that require an async engine
# and convert async engines to sync engines temporarily for tests setup operations
# and such
async_to_sync_drivers_dict = {'asyncpg':'postgresql+psycopg2',
                              'aiosqlite':'sqlite',
                              'aiomysql':'mysql+pymysql'}


# +
def execute_coroutine_sync(coro):
    # for pytest we have to create a new event loop
    # otherwise it says there is no event loop for the main thread
    # and if I am not mistaken, after it has been created we will
    # use `get_event_loop` to retrieve it afterwards instead of
    # each time recreating an event loop which would cause problems
    # with distinct futures?
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop=loop)
    task = asyncio.ensure_future(coro, loop=loop)
    return loop.run_until_complete(task)


def sync_async_exec_switch(func, *args, **kwargs):
    """
    Executes given function with given parameters
    synchronously even if it is a coroutine.
    This allows a coroutine to be used in a synchronous function
    """
    if inspect.iscoroutine(func) or inspect.iscoroutinefunction(func):
        return execute_coroutine_sync(func(*args, **kwargs))
    elif callable(func):
        return func(*args, **kwargs)
    else:
        raise TypeError('Expected a coroutine or callable')


def is_async_sqla_obj(obj):
    """
    Returns True if `obj` is an asynchronous sqlalchemy connectable (engine or connection)
    otherwise False.
    """
    # sqla < 1.4 does not support asynchronous connectables
    if not _sqla_gt14():
        return False
    from sqlalchemy.ext.asyncio.engine import AsyncConnection, AsyncEngine
    return isinstance(obj, (AsyncConnection, AsyncEngine))


# repeat all arguments even if it's stupid, this is the last thing I want to troubleshoot :|...
# todo: is there a way I can compare equality of parameters? in case I change parameters the parameters of
# `upsert` (which are the same as `aupsert`) I want to get an error if I don't change them here as well
def upsert_or_aupsert(con,
                      df:pd.DataFrame,
                      table_name:str,
                      if_row_exists:str,
                      schema:Optional[str]=None,
                      create_schema:bool=False,
                      create_table:bool=True,
                      add_new_columns:bool=False,
                      adapt_dtype_of_empty_db_columns:bool=False,
                      chunksize:Optional[int]=None,
                      dtype:Union[dict,None]=None,
                      yield_chunks:bool=False):
    """
    Detects if given connectable is asynchronous and automatically
    executes the appropriate upsert function (`pangres.upsert` if
    synchronous and `pangres.aupsert` if asynchronous).

    Examples
    --------
    >>> import pandas as pd
    >>> from pangres import DocsExampleTable
    >>>
    >>> df = DocsExampleTable.df
    >>>
    >>> # sync engine
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("sqlite://")
    >>> upsert_or_aupsert(con=engine, df=df, table_name='example', if_row_exists='update')
    >>>
    >>> # async engine
    >>> from sqlalchemy.ext.asyncio import create_async_engine  # doctest: +SKIP
    >>> engine = create_async_engine("postgresql+asyncpg://username:password@localhost:5432/postgres")  # doctest: +SKIP
    >>> upsert_or_aupsert(con=engine, df=df, table_name='example', if_row_exists='update')  # doctest: +SKIP
    """
    f = aupsert if is_async_sqla_obj(con) else upsert
    return sync_async_exec_switch(func=f, con=con, df=df, table_name=table_name, if_row_exists=if_row_exists,
                                  schema=schema, create_schema=create_schema, create_table=create_table,
                                  add_new_columns=add_new_columns,
                                  adapt_dtype_of_empty_db_columns=adapt_dtype_of_empty_db_columns,
                                  chunksize=chunksize, dtype=dtype, yield_chunks=yield_chunks)


def create_sync_or_async_engine(conn_string, **kwargs):
    """
    Automatically creates an appropriate engine for given connection string
    (synchronous or asynchronous).

    Examples
    --------
    >>> # sync
    >>> engine = create_sync_or_async_engine("sqlite://")
    >>> # async
    >>> engine = create_sync_or_async_engine("postgresql+asyncpg://username:password@localhost:5432/postgres")  # doctest: +SKIP
    """
    # if we see any known async drivers we will create an async engine
    if any(s in conn_string.split('/')[0] for s in async_to_sync_drivers_dict):
        if not _sqla_gt14():
            raise NotImplementedError('Asynchronous engines require sqlalchemy >= 1.4')

        from sqlalchemy.ext.asyncio import create_async_engine
        return create_async_engine(conn_string)
    # otherwise we will just assume we have to create a sync engine
    else:
        return create_engine(conn_string)


def async_engine_to_sync_engine(engine):
    """
    If an engine is async, creates a new engine that is synchronous.
    It does that by switching to a synchronous driver for given database:
    * asyncpg -> psycopg2
    * aiosqlite -> sqlite3
    * ...

    See variable `async_to_sync_drivers_dict` in this module.

    This is probably not super elegant but it saves a lot of hassle for
    everything that "surround" tests like setups, verifications, cleaning up...

    For the "proper" testing part though this should obviously not be used
    as it would defeat the purpose! E.g. in the presence of an engine using
    `asyncpg` we will want to test it with `aupsert`. We are not going to convert
    it to a synchronous engine and pass it to `upsert`...
    """
    u = engine.url
    params = {attr:getattr(u, attr) for attr in ('drivername', 'username', 'password', 'host', 'port', 'database', 'query')}
    # case where it is already sync
    if not is_async_sqla_obj(engine):
        return engine
    # case where it is async but we don't recognize the driver
    if not any(s in params['drivername'] for s in async_to_sync_drivers_dict):
        raise NotImplementedError(f'Cannot create a sync engine from this unknown async engine: {engine}')
    #  convert to a sync engine
    for s in async_to_sync_drivers_dict:
        if s in params['drivername']:
            params['drivername'] = async_to_sync_drivers_dict[s]
            break
    new_u = URL.create(**params)
    return create_engine(str(new_u))


@contextmanager
def sync_async_connect_switch(engine):
    """
    Context manager for connecting to an engine synchronously
    even if it is of asynchronous type
    """
    # I thought I could use `pangres.tests.conftest.sync_async_exec_switch`
    # but `engine.connect()` with an asynchronous engine
    # is not recognized by inspect as a coroutine :|
    if is_async_sqla_obj(engine):
        try:
            connection = execute_coroutine_sync(engine.connect())
            yield connection
        finally:
            execute_coroutine_sync(connection.close())
    else:
        try:
            connection = engine.connect()
            yield connection
        finally:
            connection.close()


# -

# ## Misc

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


def parse_params_parameterize(params_names, params_values):
    """
    Parses parameters similarly to pytest.mark.parameterize.
    This is a helper for function `parameterize_async` (see below).

    Examples
    --------
    >>> parse_params_parameterize("a", ['foo', 'bar', ['foo', 'bar'], {0:'foo'}])
    [{'a': 'foo'}, {'a': 'bar'}, {'a': ['foo', 'bar']}, {'a': {0: 'foo'}}]

    >>> parse_params_parameterize("a, b", [['foo', 'bar'], [{0:'foo'}, 'foo']])
    [{'a': 'foo', 'b': 'bar'}, {'a': {0: 'foo'}, 'b': 'foo'}]
    """
    assert isinstance(params_values, list)
    names = params_names.replace(' ', '').split(',')
    assert len(names) > 0
    # handle easy case of just one name
    if len(names) == 1:
        return [{names[0]:val} for val in params_values]
    else:
        l = []
        for values in params_values:
            assert isinstance(values, list)
            assert len(values) == len(names)
            l.append({name:value for name, value in zip(names, values)})
        return l


def parameterize_async(params_names, params_values):
    """
    This is a dumbed down version of parameterize for async tests because
    I could not get this to work otherwise (the first iteration of an async
    test would work but not the second and further iterations because the same
    event loop is reused).

    Thanks to https://stackoverflow.com/a/42581103 for making decorators with arguments

    Unlike pytest.mark.parameterize this will not print different tests :(.
    Also this is quite hacky and I am not happy about it... But at least it works.

    **IMPORTANT**

    Arguments that need to be parameterized need to be set to `None`!
    Otherwise pytest will look for fixtures and such and complain it cannot
    find a value for a given argument.
    """
    def sub_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # make sure param names are in the func
            sig = signature(func)
            names = params_names.replace(' ', '').split(',')
            for name in names:
                assert name in sig.parameters

            # get params
            params = parse_params_parameterize(params_names=params_names,
                                               params_values=params_values)

            # execute func
            for kwargs_modifications in params:
                new_kwargs = kwargs.copy()
                new_kwargs.update(kwargs_modifications)
                sync_async_exec_switch(func, *args, **new_kwargs)
            return
        return wrapper
    return sub_decorator


# -

# ## SQL

# +
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
    engine = async_engine_to_sync_engine(engine)
    ns = f'{schema}.{table_name}' if schema is not None else table_name
    with engine.connect() as con:
        # check if the table is present
        if table_exists(connection=con, schema=schema, table_name=table_name):
            return pd.read_sql(text(f'SELECT * FROM {ns}'), con=con, **read_sql_kwargs)
        elif error_if_missing:
            raise AssertionError(f'Table {ns} does not exist')
        else:
            return None


def drop_schema(engine, schema):
    # temporarily create a synchronous engine if in presence if an asynchronous engine
    # (e.g. asyncpg -> psycopg2)
    engine = async_engine_to_sync_engine(engine)
    with engine.connect() as connection:
        connection.execute(text(f'DROP SCHEMA IF EXISTS {schema};'))
        if hasattr(connection, 'commit'):
            connection.commit()


def drop_table(engine, schema, table_name):
    # temporarily create a synchronous engine if in presence if an asynchronous engine
    # (e.g. asyncpg -> psycopg2)
    engine = async_engine_to_sync_engine(engine)
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


# thanks to https://stackoverflow.com/a/42581103 for making decorators with arguments
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
            # note that this could be a coroutine :|
            sync_async_exec_switch(function, *args, **kwargs)

            # after test
            if drop_after_test:
                drop_table(engine=engine, schema=schema, table_name=table_name)

            # dispose of engine to free up connections
            sync_async_exec_switch(engine.dispose)
            return
        return wrapper
    return sub_decorator


def get_table_namespace(schema, table_name):
    return f'{schema}.{table_name}' if schema is not None else table_name


def commit(connection):
    if hasattr(connection, 'commit'):
        connection.commit()


# -

# ## Variables for tests

# +
class TableNames:
    ADD_NEW_COLUMN = 'test_add_new_column'
    BAD_COLUMN_NAMES = 'test_bad_column_names'
    BAD_TEXT = 'test_bad_text'
    BENCHMARK = 'test_speed'
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
    PK_MYSQL = 'test_pk_mysql'  # for checking if autoincrement is disabled in MySQL
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

# ## Function to read back from database data we inserted
# We need to apply a few modification for comparing DataFrames we get back from the DB and DataFrames we expect e.g. for JSON (with SQlite pandas reads it as string).

def read_example_table_from_db(engine, schema, table_name):
    engine = async_engine_to_sync_engine(engine)
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


# # Tests generation

# +
class TestFunctionInfo:
    """
    Provides some kind of metadata/info for test functions

    Examples
    --------
    >>> # whether a test function has an async equivalent
    >>> module_namespace = 'pangres.tests.test_yield_chunks'
    >>> TestFunctionInfo(module_namespace=module_namespace,
    ...                  function_name='test_get_nb_rows').has_async_variant
    True
    >>> TestFunctionInfo(module_namespace=module_namespace,
    ...                  function_name='test_get_nb_rows_async').is_async
    True
    >>> TestFunctionInfo(module_namespace=module_namespace,
    ...                  function_name='test_get_nb_rows').is_async
    False
    """

    def __init__(self, module_namespace, function_name):
        self.module_namespace = module_namespace
        self.function_name = function_name

    @property
    def is_async(self):
        # do not ask if it is a coroutine, this is
        # a convention where a test function will
        # have an asynchronous variant with the suffix `async`
        # sync engines will only go through the sync variant
        # while async engines will only go through the async variant
        return self.function_name.endswith('_async')

    @property
    def has_async_variant(self):
        # makes no sense to ask this if we are already refering to the async variant
        if self.is_async:
            raise AssertionError('This is the async variant')
        m = importlib.import_module(self.module_namespace)
        return hasattr(m, f'{self.function_name}_async')


def pytest_addoption(parser):
    parser.addoption('--sqlite_conn', action="store", type=str, default=None)
    parser.addoption('--async_sqlite_conn', action="store", type=str, default=None)
    parser.addoption('--pg_conn', action="store", type=str, default=None)
    parser.addoption('--async_pg_conn', action="store", type=str, default=None)
    parser.addoption('--mysql_conn', action="store", type=str, default=None)
    parser.addoption('--async_mysql_conn', action="store", type=str, default=None)
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
                    'async_sqlite_conn':metafunc.config.option.async_sqlite_conn,
                    'pg':metafunc.config.option.pg_conn,
                    'asyncpg':metafunc.config.option.async_pg_conn,
                    'mysql':metafunc.config.option.mysql_conn,
                    'async_mysql_conn':metafunc.config.option.async_mysql_conn}
    if not any(v is not None for v in conn_strings.values()):
        raise ValueError('You must provide at least one connection string (e.g. argument --sqlite_conn)!')

    engines, schemas, ids = [], [], []
    for db_type, conn_string in conn_strings.items():

        # cases where we don't skip tests generation
        if conn_string is None:
            continue

        # get engine and schema
        schema = metafunc.config.option.pg_schema if db_type in ('pg', 'asyncpg') else None
        engine = create_sync_or_async_engine(conn_string)

        # skip async tests for sync engines
        test_func_info = TestFunctionInfo(module_namespace=metafunc.module.__name__,
                                          function_name=metafunc.function.__name__)
        is_async_engine = is_async_sqla_obj(engine)
        if test_func_info.is_async:
            if not is_async_engine:
                continue

        # skip sync tests for async engines when a tests module has an async variant
        elif test_func_info.has_async_variant and is_async_engine:
            continue

        # generate tests
        schemas.append(schema)
        engines.append(engine)
        schema_id = '' if schema is None else f'_schema:{schema}'
        ids.append(f'{engine.url.drivername}{schema_id}')
        # for sqlalchemy 1.4+ use future=True to try the future sqlalchemy 2.0
        # do not do this for async engines which already implement 2.0 functionalities
        if _sqla_gt14() and not is_async_engine:
            future_engine = create_engine(conn_string, future=True)
            schemas.append(schema)
            engines.append(future_engine)
            ids.append(f'{engine.url.drivername}{schema_id}_future')
    assert len(engines) == len(schemas) == len(ids)
    metafunc.parametrize("engine, schema", list(zip(engines, schemas)), ids=ids, scope='module')
