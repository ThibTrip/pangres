#!/usr/bin/env python
# coding: utf-8
# +
"""
This module tests we can get information back from
the upserted chunks when the parameter `yield_chunks`
is True. It also check the integrity of the data.
"""
import math
import pandas as pd
import pytest
from sqlalchemy import VARCHAR, INT

# local imports
from pangres import aupsert, upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import (drop_table_for_test, read_example_table_from_db,
                                    TableNames)


# -

# # Insert values one by one

# +
@drop_table_for_test(TableNames.WITH_YIELD)
def test_get_nb_rows(engine, schema):
    # config
    table_name = TableNames.WITH_YIELD
    nb_rows, chunksize = 20, 3
    nb_last_chunk = nb_rows % chunksize
    nb_chunks = math.ceil(nb_rows/chunksize)
    # MySQL does not want flexible text length in indices/PK
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)

    # iterate over upsert results
    # make sure we can extract the number of updated rows and that it is correct
    iterator = upsert(con=engine, df=df, table_name=table_name, if_row_exists='update',
                      schema=schema, chunksize=chunksize, dtype=dtype, yield_chunks=True)

    for ix, result in enumerate(iterator):
        assert result.rowcount == (chunksize if ix != nb_chunks-1 else nb_last_chunk)

    # verify the inserted data is as expected
    # we sort the index for MySQL
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())


# ASYNC VARIANT (identified by suffix `_async`, only executed for async engines)
@pytest.mark.asyncio
@drop_table_for_test(TableNames.WITH_YIELD)
async def test_get_nb_rows_async(engine, schema):
    # config
    table_name = TableNames.WITH_YIELD
    nb_rows, chunksize = 20, 3
    nb_last_chunk = nb_rows % chunksize
    nb_chunks = math.ceil(nb_rows/chunksize)
    # MySQL does not want flexible text length in indices/PK
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)

    # iterate over upsert results
    # make sure we can extract the number of updated rows and that it is correct
    async_gen = await aupsert(con=engine, df=df, table_name=table_name, if_row_exists='update',
                              schema=schema, chunksize=chunksize, dtype=dtype, yield_chunks=True)

    # unlike the equivalent synchronous test, enumerate(async_generator) will not work
    ix = 0
    async for result in async_gen:
        assert result.rowcount == (chunksize if ix != nb_chunks-1 else nb_last_chunk)
        ix += 1

    # verify the inserted data is as expected
    # we sort the index for MySQL
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())


# -

# # Test of an empty DataFrame

# +
@drop_table_for_test(TableNames.WITH_YIELD_EMPTY)
def test_yield_empty_df(engine, schema):
    df = pd.DataFrame({'id':[], 'value':[]}).set_index('id')

    # we should get an empty generator back
    iterator = upsert(con=engine, df=df, table_name=TableNames.WITH_YIELD_EMPTY, if_row_exists='update',
                      schema=schema, dtype={'id':INT, 'value':INT}, yield_chunks=True)

    # the for loop should never run because the generator should be empty
    for result in iterator:
        raise AssertionError('Expected the generator returned by upsert with an empty df to be empty')


# ASYNC VARIANT (identified by suffix `_async`, only executed for async engines)
@pytest.mark.asyncio
@drop_table_for_test(TableNames.WITH_YIELD_EMPTY)
async def test_yield_empty_df_async(engine, schema):
    df = pd.DataFrame({'id':[], 'value':[]}).set_index('id')

    # we should get an empty generator back
    async_gen = await aupsert(con=engine, df=df, table_name=TableNames.WITH_YIELD_EMPTY, if_row_exists='update',
                              schema=schema, dtype={'id':INT, 'value':INT}, yield_chunks=True)

    # the for loop should never run because the generator should be empty
    async for result in async_gen:
        raise AssertionError('Expected the generator returned by upsert with an empty df to be empty')
