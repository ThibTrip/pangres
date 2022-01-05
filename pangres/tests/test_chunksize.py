#!/usr/bin/env python
# coding: utf-8
"""
This module tests if uploading data in chunks works as
expected (we should get the correct DataFrame length back).
"""
import pandas as pd
import pytest
from sqlalchemy import VARCHAR
from pangres import upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import drop_table_for_test, read_example_table_from_db, TableNames


# # Helpers

@drop_table_for_test(TableNames.VARIOUS_CHUNKSIZES)
def insert_chunks(engine, schema, chunksize, nb_rows):
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)
    # MySQL does not want flexible text length in indices/PK
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    upsert(schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES, df=df, chunksize=chunksize,
           engine=engine, if_row_exists='update', dtype=dtype)
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES)
    # sort index (for MySQL...)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())


# # Tests

# ## Insert values one by one

def test_insert_one(engine, schema):
    insert_chunks(engine, schema, chunksize=1, nb_rows=11)


# ## Insert an odd size of chunks that is not a multiple of df length

def test_insert_odd_chunksize(engine, schema):
    insert_chunks(engine, schema, chunksize=3, nb_rows=11)

