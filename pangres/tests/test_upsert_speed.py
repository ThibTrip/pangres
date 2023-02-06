#!/usr/bin/env python
# coding: utf-8
# +
import pytest
from pandas import __version__ as pandas_version
from sqlalchemy import __version__ as sqla_version
from typing import Any, Dict
# local imports
from pangres import aupsert, upsert
from pangres.helpers import _version_equal_or_greater_than
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import (adrop_table, drop_table_between_tests, drop_table,
                                    execute_coroutine_sync, sync_or_async_test, TableNames)
from pangres.utils import adjust_chunksize


# -


# # Helpers

def skip_if_sqlalchemy_pandas_conflict():
    """
    pandas >= 1.4.0 requires sqlalchemy >= 1.4.0. We will
    skip tests where pandas is required if these conditions
    are not met
    """
    # pandas >= 1.4.0 requires sqlalchemy >= 1.4.0
    pd_1_4_0 = _version_equal_or_greater_than(pandas_version, '1.4.0')
    sqla_1_4_0 = _version_equal_or_greater_than(sqla_version, '1.4.0')
    if pd_1_4_0 and not sqla_1_4_0:
        pytest.skip('pandas >= 1.4.0 requires sqlalchemy >= 1.4.0 '
                    f'(installed: sqlalchemy v{sqla_version}, pandas v{pandas_version})')


# # Sync and async variants for tests
#
# Note that we cannot use the same strategy ((`run_test_foo`|`run_test_foo_async`) -> `test_foo`) as usual here
# because `pytest-benchmark` which is used inside of both sync and async tests must run sync.
#
# If we were to use the event loop for the async test then we would have to create nested event loops for running
# asynchronous benchmark subfunctions (we cannot use the already running loop).
#
# This is possible with `nest_asyncio` but that just complicates things.
#
# What we will then do is use synchronous functions for testing asynchronous engines. And we will make
# synchronous versions of the benchmark subfunctions.

# iterations has to be 1 when there is a setup
pytest_params: Dict[str, Any] = dict(argnames='nb_rows, rounds, iterations', argvalues=[[10, 5, 1], [1_000, 1, 1]],
                                     ids=['many_little_inserts', 'big_insert'])

# ## "Normal" insert speed
#
# Case where the table does not exist yet so there is no pk to compare.


# +
@drop_table_between_tests(table_name=TableNames.BENCHMARK_INSERT)
def run_test_create_and_insert_speed(engine, schema, benchmark, library, nb_rows, rounds, iterations):
    # get a df (we don't test JSON as this is problematic with pandas)
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows).drop(columns=['favorite_colors'])

    # prepare setup function
    table_name = TableNames.BENCHMARK_INSERT

    def setup():
        drop_table(engine=engine, schema=schema, table_name=table_name)

    # prepare func for benchmark
    chunksize = adjust_chunksize(con=engine, df=df, chunksize=nb_rows)
    if library == 'pangres':
        def benchmark_func():  # pragma: no cover
            upsert(con=engine, df=df, schema=schema, chunksize=chunksize, table_name=table_name,
                   if_row_exists='update')

    elif library == 'pandas':
        skip_if_sqlalchemy_pandas_conflict()

        def benchmark_func():  # pragma: no cover
            # create table
            df.to_sql(con=engine, schema=schema, name=table_name, method='multi',
                      chunksize=chunksize)

    # benchmark
    try:
        benchmark.pedantic(benchmark_func, setup=setup, rounds=rounds, iterations=iterations)
    except NotImplementedError as e:  # pragma: no cover
        if 'not implemented for SQLAlchemy 2' in str(e):
            pytest.skip('in Python 3.6 there is some kind of problem with engines created with '
                        '`future=True` flag and pandas')


# IMPORTANT1: not "async def"!
# IMPORTANT2: no decorator for dropping table!
def run_test_create_and_insert_speed_async(engine, schema, benchmark, library, nb_rows, rounds, iterations):
    if library == 'pandas':
        pytest.skip('async engines will not work with pandas')

    # get a df (we don't test JSON as this is problematic with pandas)
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows).drop(columns=['favorite_colors'])

    # prepare setup function (make it synchronous even if the engine is asynchronous)
    table_name = TableNames.BENCHMARK_INSERT

    def drop_table_sync():
        execute_coroutine_sync(adrop_table(engine=engine, schema=schema, table_name=table_name))

    setup = drop_table_sync

    # prepare func for benchmark and its synchronous alternative
    chunksize = adjust_chunksize(con=engine, df=df, chunksize=nb_rows)
    if library == 'pangres':

        async def abenchmark_func():  # pragma: no cover
            await aupsert(con=engine, df=df, schema=schema, chunksize=chunksize,
                          table_name=table_name, if_row_exists='update')

        def benchmark_func():  # pragma: no cover
            execute_coroutine_sync(abenchmark_func())

    else:  # pragma: no cover
        raise AssertionError('This test can only work for the `pangres` library')

    # benchmark
    try:
        benchmark.pedantic(benchmark_func, setup=setup, rounds=rounds, iterations=iterations)
    except NotImplementedError as e:  # pragma: no cover
        if 'not implemented for SQLAlchemy 2' in str(e):
            pytest.skip('in Python 3.6 there is some kind of problem with engines created with '
                        '`future=True` flag and pandas')
        else:
            raise e
    finally:
        drop_table_sync()


# -

# ## Upsert overwrite speed
#
# This feature is not available in pandas yet

# +
@drop_table_between_tests(table_name=TableNames.BENCHMARK_UPSERT)
def run_test_upsert_speed(engine, schema, benchmark, library, nb_rows, rounds, iterations, if_row_exists):
    assert library == 'pangres'  # in case pandas changes and we forget to update the tests

    # get a df
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows).drop(columns=['favorite_colors'])

    # prepare setup function
    chunksize = adjust_chunksize(con=engine, df=df, chunksize=nb_rows)
    table_name = TableNames.BENCHMARK_UPSERT
    common_kwargs_upsert = dict(con=engine, df=df, schema=schema, chunksize=chunksize,
                                table_name=table_name, if_row_exists=if_row_exists)

    def setup():
        drop_table(engine=engine, schema=schema, table_name=table_name)
        # create with data so that we can measure the impact of looking up and overwriting or
        # ignoring records based on primary keys
        upsert(**common_kwargs_upsert, create_table=True)

    # prepare func for benchmark
    # insert update/ignore with `create_table=False` to maximise speed
    def benchmark_func():  # pragma: no cover
        upsert(**common_kwargs_upsert, create_table=False)

    # benchmark
    benchmark.pedantic(benchmark_func, setup=setup, rounds=rounds, iterations=iterations)


# IMPORTANT1: not "async def"!
# IMPORTANT2: no decorator for dropping table!
def run_test_upsert_speed_async(engine, schema, benchmark, library, nb_rows, rounds, iterations, if_row_exists):
    assert library == 'pangres'  # in case pandas changes and we forget to update the tests

    # get a df
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows).drop(columns=['favorite_colors'])

    # prepare setup function (make it synchronous even if the engine is asynchronous)
    chunksize = adjust_chunksize(con=engine, df=df, chunksize=nb_rows)
    table_name = TableNames.BENCHMARK_UPSERT
    common_kwargs_upsert = dict(con=engine, df=df, schema=schema, chunksize=chunksize,
                                table_name=table_name, if_row_exists=if_row_exists)

    def drop_table_sync():
        execute_coroutine_sync(adrop_table(engine=engine, schema=schema, table_name=table_name))

    def setup():
        drop_table_sync()
        # create with data so that we can measure the impact of looking up and overwriting or
        # ignoring records based on primary keys
        execute_coroutine_sync(aupsert(**common_kwargs_upsert, create_table=True))

    # prepare func for benchmark
    # insert update/ignore with `create_table=False` to maximise speed
    def benchmark_func():  # pragma: no cover
        execute_coroutine_sync(aupsert(**common_kwargs_upsert, create_table=False))

    # benchmark
    try:
        benchmark.pedantic(benchmark_func, setup=setup, rounds=rounds, iterations=iterations)
    finally:
        drop_table_sync()


# -

# # Actual tests

# +
@pytest.mark.parametrize('library', ['pandas', 'pangres'])
@pytest.mark.parametrize(**pytest_params)
def test_create_and_insert(engine, schema, benchmark, library, nb_rows, rounds, iterations):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_create_and_insert_speed_async,
                       f_sync=run_test_create_and_insert_speed,
                       benchmark=benchmark, library=library,
                       nb_rows=nb_rows, rounds=rounds, iterations=iterations)


@pytest.mark.parametrize('library', ['pangres'])
@pytest.mark.parametrize('if_row_exists', ['update', 'ignore'])
@pytest.mark.parametrize(**pytest_params)
def test_upsert_speed(engine, schema, benchmark, library, nb_rows, rounds, iterations, if_row_exists):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_upsert_speed_async,
                       f_sync=run_test_upsert_speed,
                       benchmark=benchmark, library=library,
                       nb_rows=nb_rows, rounds=rounds, iterations=iterations,
                       if_row_exists=if_row_exists)
