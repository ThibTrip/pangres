#!/usr/bin/env python
# coding: utf-8
# +
"""
Configuration and helpers for the tests of pangres with pytest.
"""
import pandas as pd
import pytest
import json
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy import inspect as sqla_inspect

from pangres.helpers import _sqla_gt14


# -

# # Helpers

# ## ConnectionString object

class ConnectionString:
    def __init__(self, string, is_async=False, schema=None):
        self._string = string
        self._is_async = is_async
        self._schema = schema

    @property
    def string(self):
        return self._string

    @property
    def is_async(self):
        return self._is_async

    @property
    def schema(self):
        return self._schema


    def get_engine(self, **kwargs):
        if not self._is_async:
            return create_engine(self._string, **kwargs)
        else:
            if not _sqla_gt14():
                from sqlalchemy import __version__ as sqla_version
                raise NotImplementedError('Async engines require sqlalchemy >= 1.4. '
                                          f'Current version is {sqla_version}')
            else:
                from sqlalchemy.ext.asyncio import create_async_engine
                return create_async_engine(self._string, **kwargs)


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


# -

# # Pytest functions

# +
def pytest_addoption(parser):
    parser.addoption('--sqlite_conn', action="store", default=None)
    parser.addoption('--pg_conn', action="store", default=None)
    parser.addoption('--mysql_conn', action="store", default=None)
    parser.addoption('--pg_conn_async', action="store", default=None)
    parser.addoption('--mysql_conn_async', action="store", default=None)
    parser.addoption('--pg_schema', action='store', default=None)

# this is called for every test
def pytest_generate_tests(metafunc):
    options = metafunc.config.option
    pg_schema = options.pg_schema if options.pg_schema is not None else 'public'
    conn_strings = [ConnectionString(string=options.sqlite_conn),
                    ConnectionString(string=options.pg_conn, schema=pg_schema),
                    ConnectionString(string=options.mysql_conn),
                    ConnectionString(string=options.pg_conn_async, is_async=True, schema=pg_schema),
                    ConnectionString(string=options.mysql_conn_async, is_async=True)]

    # filter connection strings (not provided ones will be None)
    conn_strings = [c for c in conn_strings if c.string is not None]
    if len(conn_strings) == 0:
        raise ValueError('You must provide at least one connection string (e.g. argument --sqlite_conn)!')

    # prepare parameters for tests
    engines, schemas, ids = [], [], []
    is_async_test_module = metafunc.module.__name__.endswith('_async')
    for conn_string in conn_strings:
        # skip sync tests for async engines
        if conn_string.is_async and not is_async_test_module:
            continue
        # skip async tests for sync engines
        if not conn_string.is_async and is_async_test_module:
            continue

        # get engine and schemas that we will use as parameters for all tests
        engine, schema = conn_string.get_engine(), conn_string.schema
        engines.append(engine)
        schemas.append(schema)
        ids.append(f'{engine.url.drivername}_{schema}')

        # for sqlalchemy 1.4+ use future=True to try the future sqlalchemy 2.0
        # (only for non async engines)
        if _sqla_gt14() and not conn_string.is_async:
            future_engine = conn_string.get_engine(future=True)
            engines.append(future_engine)
            schemas.append(schema)
            ids.append(f'{future_engine.url.drivername}_FUTURE_{schema}')

    # verifications
    assert len(engines) == len(schemas) == len(ids)

    # handle case when there is nothing to test
    if len(engines) == 0:
        pytest.skip()

    # generate tests
    params = list(zip(engines, schemas))
    metafunc.parametrize("engine, schema", params, ids=ids, scope='module')


# -

# ## Tool for reading example data we inserted in the database

class ReaderSQLExampleTables:
    """
    Tool for reading and then wrangling example tables we saved in SQL
    databases for our tests.
    This is necessary because we can get different data
    back than what we would expect e.g. for JSON in SQlite,
    pandas will read it as a string.
    After we have applied this sort of normalization
    we can compare DataFrames in memory with tables
    in SQL databases.
    """
    def _wrangle(df):
        json_convert = lambda obj: json.loads(obj) if isinstance(obj, str) else obj
        return (df.astype({'likes_pizza':bool})
                .assign(timestamp=lambda df: pd.to_datetime(df['timestamp'], utc=True))
                .assign(favorite_colors=lambda df: df['favorite_colors'].map(json_convert)))


    def read(engine, schema, table_name):
        namespace = f'{schema}.{table_name}' if schema is not None else table_name
        with engine.connect() as connection:
            df_db = pd.read_sql(text(f'SELECT * FROM {namespace}'), con=connection, index_col='profileid')
            return ReaderSQLExampleTables._wrangle(df_db)


    # async variant
    async def aread(engine, schema, table_name):
        namespace = f'{schema}.{table_name}' if schema is not None else table_name
        async with engine.connect() as connection:
            # pandas does not support async engines yet
            proxy = await connection.execute(text(f'SELECT * FROM {namespace}'))
            results = [r._asdict() for r in proxy.all()]
            df_db = pd.DataFrame(results).set_index('profileid')
            return ReaderSQLExampleTables._wrangle(df_db)
