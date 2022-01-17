#!/usr/bin/env python
# coding: utf-8
# +
import pytest
from math import floor
from sqlalchemy import VARCHAR

# local imports
from pangres.helpers import _sqlite_gt3_32_0
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import (drop_table_for_test, drop_table,
                                    is_async_sqla_obj, TableNames, upsert_or_aupsert)


# -


# # Helpers

# +
def create_or_upsert_with_pangres(engine, schema, if_row_exists, df, chunksize, **kwargs):
    # MySQL does not want flexible text length in indices/PK
    dtype={'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    upsert_or_aupsert(con=engine, df=df, schema=schema, chunksize=chunksize,
                      table_name=TableNames.BENCHMARK, if_row_exists=if_row_exists,
                      dtype=dtype, **kwargs)


def create_with_pandas(engine, schema, df):
    dtype={'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None

    # we need this for SQlite when using pandas table creation
    # since we cannot use more than X parameters in a parameterized query
    if 'sqlite' in engine.dialect.dialect_description:
        max_params = 32766 if _sqlite_gt3_32_0() else 999
        col_len = len(df.columns) + len(df.index.names)
        chunksize = floor(max_params / col_len)
    else:
        chunksize = None

    # create table
    df.to_sql(con=engine, schema=schema, name=TableNames.BENCHMARK, method='multi',
              chunksize=chunksize, dtype=dtype)


# -

# # Tests

pytest_params = dict(argnames='nb_rows, rounds, iterations', argvalues=[[10, 5, 1], [1_000, 1, 1]],
                     ids=['many_little_inserts', 'big_insert'])

# ## "Normal" insert speed
#
# Case where the table does not exist yet so there is no pk to compare.


@pytest.mark.parametrize('library', ['pandas', 'pangres'])
@pytest.mark.parametrize(**pytest_params)
@drop_table_for_test(table_name=TableNames.BENCHMARK)
def test_create_and_insert_speed(engine, schema, benchmark, library, nb_rows, rounds, iterations):
    # skip async engines with pandas
    if is_async_sqla_obj(engine) and library == 'pandas':
        pytest.skip('async engines will not work with pandas')

    # get a df
    # we don't test JSON as this is problematic with pandas
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows).drop(columns=['favorite_colors'])

    # prepare funcs for benchmark and then do the benchmark
    switch = {'pangres':lambda: create_or_upsert_with_pangres(engine=engine, schema=schema, if_row_exists='update',
                                                              df=df, chunksize=nb_rows),
              'pandas':lambda: create_with_pandas(engine=engine, schema=schema, df=df)}

    try:
        benchmark.pedantic(switch[library], setup=lambda: drop_table(engine=engine, schema=schema,
                                                                     table_name=TableNames.BENCHMARK),
                           rounds=rounds, iterations=iterations)
    except NotImplementedError as e:
        if 'not implemented for SQLAlchemy 2' in str(e):
            pytest.skip('in Python 3.6 there is some kind of problem with engines created with '
                        '`future=True` flag and pandas')


# ## Upsert overwrite speed

# this feature is not available in pandas yet
@pytest.mark.parametrize('library', ['pangres'])
@pytest.mark.parametrize('if_row_exists', ['update', 'ignore'])
@pytest.mark.parametrize(**pytest_params)
@drop_table_for_test(table_name=TableNames.BENCHMARK)
def test_upsert_speed(engine, schema, benchmark, library, nb_rows, rounds, iterations, if_row_exists):
    assert library == 'pangres'  # in case pandas changes and we forget to update the tests

    # skip async engines with pandas
    if is_async_sqla_obj(engine) and library == 'pandas':
        pytest.skip('async engines will not work with pandas')

    # get a df
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows).drop(columns=['favorite_colors'])

    # setup for test (create table with no rows)
    def setup():
        create_or_upsert_with_pangres(engine=engine, schema=schema, if_row_exists=if_row_exists,
                                      df=df.head(0), chunksize=nb_rows)

    # test func
    # insert update/ignore with `create_table=False` to maximise speed
    func = lambda: create_or_upsert_with_pangres(engine=engine, schema=schema, if_row_exists=if_row_exists,
                                                 df=df, chunksize=nb_rows, create_table=False)

    benchmark.pedantic(func, setup=setup, rounds=rounds, iterations=iterations)
