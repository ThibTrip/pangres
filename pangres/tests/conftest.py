#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import asyncio
import inspect
import pandas as pd
import sqlalchemy as sa
from functools import wraps
from inspect import signature
from sqlalchemy import create_engine, text
from sqlalchemy.sql.compiler import IdentifierPreparer
from typing import Union

# local imports
from pangres.helpers import _sqla_gt14
# -

# # Helpers for other test modules

# ## Async/Sync detection and handling

# +
async_sql_drivers = ('asyncpg', 'aiosqlite', 'aiomysql')


def execute_coroutine_sync(coro):
    """
    Retrieves or creates an event loop and executes given coroutine
    synchronously. The strategy is as follows:

    1. try to find an event loop (it may be an event loop we previously created)
    2. if there is no event loop (this is the case when pytest starts it seems), create one
    3. execute coro
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:  # pragma: no cover
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop=loop)
    task = asyncio.ensure_future(coro, loop=loop)
    return loop.run_until_complete(task)


def sync_async_exec_switch(func, *args, **kwargs):
    """
    Executes given function with given parameters synchronously regardless
    whether it is a coroutine or not.

    Examples
    --------
    >>> def foo():
    ...     print('test sync')
    >>>
    >>> async def foo_async():
    ...     print('test async')
    >>>
    >>> sync_async_exec_switch(foo)
    test sync
    >>> sync_async_exec_switch(foo_async)
    test async
    """
    if inspect.iscoroutine(func) or inspect.iscoroutinefunction(func):
        return execute_coroutine_sync(func(*args, **kwargs))
    elif callable(func):
        return func(*args, **kwargs)
    else:  # pragma: no cover
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


def create_sync_or_async_engine(conn_string, **kwargs):
    """
    Automatically creates an appropriate engine for given connection string
    (synchronous or asynchronous).

    Examples
    --------
    >>> # sync
    >>> engine = create_sync_or_async_engine("sqlite://")
    >>> # async
    >>> conn_string_async = "postgresql+asyncpg://username:password@localhost:5432/postgres"
    >>> engine = create_sync_or_async_engine(conn_string_async)  # doctest: +SKIP
    """
    # if we see any known async drivers we will create an async engine
    if any(s in conn_string.split('/')[0] for s in async_sql_drivers):
        if not _sqla_gt14():
            raise NotImplementedError('Asynchronous engines require sqlalchemy >= 1.4')

        from sqlalchemy.ext.asyncio import create_async_engine
        return create_async_engine(conn_string)
    # otherwise we will just assume we have to create a sync engine
    else:
        return create_engine(conn_string)


# -

# ## SQL

# ### Misc

# +
def quote_object_name(con, object_name):
    """
    For quoting SQL schema names and SQL table names to protect
    against SQL injection. If you have a better idea for achieving
    that please post an issue.
    """
    return IdentifierPreparer(dialect=con.dialect).quote(object_name)


def get_table_namespace(con, schema, table_name):
    """
    Gets quoted table namespace (`schema.table_name`) to protect against
    SQL injection.
    """
    schema = quote_object_name(con=con, object_name=schema) if schema is not None else None
    table_name = quote_object_name(con=con, object_name=table_name)
    return f'{schema}.{table_name}' if schema is not None else table_name


def table_exists(connection, schema, table_name) -> bool:
    """
    Checks for SQL table existence
    """
    insp = sa.inspect(connection)
    if _sqla_gt14():
        return insp.has_table(schema=schema, table_name=table_name)
    else:
        return table_name in insp.get_table_names(schema=schema)


def commit(connection):
    """
    This is for newer versions of sqlalchemy (2.0 or 1.4 using future engines)
    where connections start with implicit transactions and have a commit
    attribute
    """
    if hasattr(connection, 'commit'):
        connection.commit()


# -

# ### SELECT

# +
def select_table(engine, schema, table_name,
                 error_if_missing=True,
                 **read_sql_kwargs) -> Union[pd.DataFrame, None]:
    """
    Does a simple SELECT * FROM {table} and returns a DataFrame from that.
    Has an option to return None if the table does not exist and
    `error_if_missing` is False.
    """
    ns = get_table_namespace(con=engine, schema=schema, table_name=table_name)
    with engine.connect() as connection:
        # check if the table is present
        if table_exists(connection=connection, schema=schema, table_name=table_name):
            return pd.read_sql(text(f'SELECT * FROM {ns}'), con=connection, **read_sql_kwargs)
        elif error_if_missing:  # pragma: no cover
            raise AssertionError(f'Table {ns} does not exist')
        else:
            return None


async def aselect_table(engine, schema, table_name,
                        error_if_missing=True,
                        **read_sql_kwargs) -> Union[pd.DataFrame, None]:
    """
    Async variant of select table
    """
    index_kwarg = 'index_col'
    for k in read_sql_kwargs:
        if k != index_kwarg:  # pragma: no cover
            raise NotImplementedError(f'Can only handle handle extra kwarg `{index_kwarg}` '
                                      f'for aselect_table. Got {k}')

    ns = get_table_namespace(con=engine, schema=schema, table_name=table_name)
    async with engine.connect() as connection:
        # check if the table is present
        exists_coro = lambda connection: table_exists(connection=connection,
                                                      schema=schema,
                                                      table_name=table_name)
        exists = await connection.run_sync(exists_coro)
        if exists:
            proxy = await connection.execute(text(f'SELECT * FROM {ns}'))
            df = pd.DataFrame(proxy)
            if index_kwarg in read_sql_kwargs:
                df.set_index(read_sql_kwargs[index_kwarg], inplace=True)
            return df
        elif error_if_missing:  # pragma: no cover
            raise AssertionError(f'Table {ns} does not exist')
        else:
            return None


# -

# ### DROP schema

# +
def drop_schema_from_conn(connection, schema):
    """
    Drops given PostgreSQL schema using connection
    """
    schema = quote_object_name(con=connection, object_name=schema)
    connection.execute(text(f'DROP SCHEMA IF EXISTS {schema};'))
    commit(connection)


async def adrop_schema_from_conn(connection, schema):
    """
    Async variant of `drop_schema_from_conn`
    """
    schema = quote_object_name(con=connection, object_name=schema)
    await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema};'))
    await connection.commit()


def drop_schema(engine, schema):
    """
    Drops given PostgreSQL schema using an engine
    """
    with engine.connect() as connection:
        drop_schema_from_conn(connection=connection, schema=schema)


async def adrop_schema(engine, schema):
    """
    Async variant of `drop_schema`
    """
    async with engine.connect() as connection:
        await adrop_schema_from_conn(connection=connection, schema=schema)


# -

# ### DROP table

# +
def drop_table_from_conn(connection, schema, table_name):
    """
    Drops given SQL table using connection
    """
    ns = get_table_namespace(con=connection, schema=schema, table_name=table_name)
    # instead of using `DROP TABLE IF EXISTS` which produces warnings
    # in MySQL when the table does exist, we will just check if it does exist
    # and using a `DROP TABLE` query accordingly
    if not table_exists(connection=connection, schema=schema, table_name=table_name):
        return
    # make sure to commit this!
    connection.execute(text(f'DROP TABLE {ns};'))
    commit(connection)


def drop_table(engine, schema, table_name):
    """
    Drops given SQL table using engine
    """
    with engine.connect() as connection:
        drop_table_from_conn(connection=connection, schema=schema, table_name=table_name)


async def adrop_table(engine, schema, table_name):
    """
    Async variant of `drop_table`
    """
    drop_coro = lambda connection: drop_table_from_conn(connection=connection, schema=schema,
                                                        table_name=table_name)
    async with engine.connect() as connection:
        await connection.run_sync(drop_coro)


def _get_function_param_value(function, param_name, args, kwargs):
    """
    Helper for function `drop_table_between_tests` to
    retrieve the value of a specific parameter
    from *args and **kwargs like when we wrap a function

    Examples
    --------
    >>> def dummy_decorator(function):
    ...     def wrapper(*args, **kwargs):
    ...         for param_name in ('a', 'b', 'c'):  # see function `foo` below
    ...             value = _get_function_param_value(function=function, param_name=param_name,
    ...                                               args=args, kwargs=kwargs)
    ...             print(f'{param_name}={value}')
    ...     return wrapper
    >>>
    >>> @dummy_decorator
    ... def foo(a, b, c):
    ...     pass
    >>>
    >>> foo(1, 2, c=3)
    a=1
    b=2
    c=3
    """
    sig = signature(function)
    assert param_name in sig.parameters, f'Parameter "{param_name}" not found in function {function.__name__}!'
    param_ix = list(sig.parameters).index(param_name)
    if param_ix <= len(args) - 1:
        return args[param_ix]
    else:
        return kwargs[param_name]


def drop_table_between_tests(table_name, drop_before_test=True, drop_after_test=True):
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
            engine = _get_function_param_value(function=function, param_name='engine', args=args, kwargs=kwargs)
            schema = _get_function_param_value(function=function, param_name='schema', args=args, kwargs=kwargs)
            # before test
            if drop_before_test:
                drop_table(engine=engine, schema=schema, table_name=table_name)
            # test
            result = function(*args, **kwargs)
            # after test
            if drop_after_test:
                drop_table(engine=engine, schema=schema, table_name=table_name)
            return result
        return wrapper
    return sub_decorator


def adrop_table_between_tests(table_name, drop_before_test=True, drop_after_test=True):
    """
    Async variant of `drop_table_between_tests`
    """
    assert drop_before_test or drop_after_test, 'One of drop_before_test or drop_after_test must be True'

    def sub_decorator(function):
        @wraps(function)
        async def wrapper(*args, **kwargs):
            # get engine and schema
            engine = _get_function_param_value(function=function, param_name='engine', args=args, kwargs=kwargs)
            schema = _get_function_param_value(function=function, param_name='schema', args=args, kwargs=kwargs)
            # before test
            if drop_before_test:
                await adrop_table(engine=engine, schema=schema, table_name=table_name)
            # test
            result = await function(*args, **kwargs)
            # after test
            if drop_after_test:
                await adrop_table(engine=engine, schema=schema, table_name=table_name)
            return result
        return wrapper
    return sub_decorator


# -

# ## Variables for tests

# +
class TableNames:
    """
    Names of tables used for tests
    """
    ADD_NEW_COLUMN = 'test_add_new_column'
    BAD_COLUMN_NAMES = 'test_bad_column_names'
    BAD_COLUMN_NAMES_PG = 'test_bad_column_names_pg'
    BAD_TEXT = 'test_bad_text'
    BENCHMARK_INSERT = 'test_speed_insert'
    BENCHMARK_UPSERT = 'test_speed_upsert'
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

# ## Function for running tests that have sync and async variants

def sync_or_async_test(engine, schema, f_async, f_sync, **kwargs):
    """
    Will execute the synchronous (Parameter `f_sync`)
    or asynchronous (Parameter `f_async`) test function
    depending on given `engine`.

    We will indeed have many tests taking the parameters
    `engine` and `schema` and having an async and a sync
    variant.

    In order for pytest not to trigger functions `f_async` and
    `f_sync` we will use a prefix `run_` for our async and sync
    variants in the test modules.

    This stategy has the following advantages:
    * we can use pytest parameterize (pytest-asyncio does not seem to support it)
    * we can also use the same test function for sync and async engines
    * in rare cases the tests function for async engines will actually be synchronous
      (e.g. for benchmarks) but this function will still work

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>>
    >>>
    >>> def run_test_connect(engine, schema):
    ...     with engine.connect():
    ...         pass
    >>>
    >>> async def run_test_connect_async(engine, schema):
    ...     with engine.connect():
    ...         pass
    >>>
    >>> def test_connect(engine, schema):
    ...     sync_or_async_test(engine=engine, schema=schema,
    ...                        f_async=run_test_connect_async,
    ...                        f_sync=run_test_connect)
    >>>
    >>> # sync
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite://')
    >>> schema = None
    >>> test_connect(engine=engine, schema=schema)
    >>>
    >>> # async
    >>> from sqlalchemy.ext.asyncio import create_async_engine  # doctest: +SKIP
    >>> engine = create_async_engine('sqlite+aiosqlite://')  # doctest: +SKIP
    >>> test_connect(engine=engine, schema=schema)  # doctest: +SKIP
    """
    test_func = f_async if is_async_sqla_obj(engine) else f_sync
    sync_async_exec_switch(func=test_func, engine=engine, schema=schema, **kwargs)


# # Tests generation
#
# Note: I thought about using `pytest_session_start` for dropping tables instead of using decorators
# to drop tables between tests but this would be problematic if the user wanted to use the same
# database for sync and async tests.

# +
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
    # only when we see the parameters "engine" and "schema" in a function
    # will we repeat a given test (at least once for each engine)
    func_params = signature(metafunc.function).parameters
    if not ('engine' in func_params and 'schema' in func_params):
        # I could not find any other way than to add a dummy
        # for executing a test only once (parameterize needs arguments)
        metafunc.parametrize('_', [''], scope='module')
        return

    # tests that we need to repeat for each engine + options (e.g. future)
    conn_strings = {'sqlite': metafunc.config.option.sqlite_conn,
                    'async_sqlite_conn': metafunc.config.option.async_sqlite_conn,
                    'pg': metafunc.config.option.pg_conn,
                    'asyncpg': metafunc.config.option.async_pg_conn,
                    'mysql': metafunc.config.option.mysql_conn,
                    'async_mysql_conn': metafunc.config.option.async_mysql_conn}
    if not any(v is not None for v in conn_strings.values()):  # pragma: no cover
        raise ValueError('You must provide at least one connection string (e.g. argument --sqlite_conn)!')

    # prepare parameters for metafunc.parametrize
    engines, schemas, ids = [], [], []
    for db_type, conn_string in conn_strings.items():

        # cases where we skip tests generation
        if conn_string is None:
            continue

        # get engine and schema
        schema = metafunc.config.option.pg_schema if db_type in ('pg', 'asyncpg') else None
        engine = create_sync_or_async_engine(conn_string)

        # generate tests
        schemas.append(schema)
        engines.append(engine)
        schema_id = '' if schema is None else f'_schema:{schema}'
        ids.append(f'{engine.url.drivername}{schema_id}')

        # for sqlalchemy 1.4+ use future=True to try the future sqlalchemy 2.0
        # do not do this for async engines which already implement 2.0 functionalities
        if _sqla_gt14() and not is_async_sqla_obj(engine):
            future_engine = create_engine(conn_string, future=True)
            schemas.append(schema)
            engines.append(future_engine)
            ids.append(f'{engine.url.drivername}{schema_id}_future')

    # generate tests
    assert len(engines) == len(schemas) == len(ids)
    metafunc.parametrize("engine, schema", list(zip(engines, schemas)), ids=ids, scope='function')
