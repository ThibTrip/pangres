#!/usr/bin/env python
# coding: utf-8
"""
This module tests we can get information back from
the upserted chunks when the parameter `yield_chunks`
is True. It also check the integrity of the data.
"""
import math
import pandas as pd
from sqlalchemy import VARCHAR
from pangres import upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import read_example_table_from_db, drop_table_if_exists


# ## Insert values one by one

def test_get_nb_rows(engine, schema):
    # params
    nb_rows, chunksize = 20, 3
    nb_last_chunk = nb_rows % chunksize
    nb_chunks = math.ceil(nb_rows/chunksize)
    table_name = 'test_yield_get_nb_rows'
    # MySQL does not want flexible text length in indices/PK
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)

    # generate example df and iterate over upsert results
    # make sure we can extract the number of updated rows and that it is correct
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)
    iterator = upsert(engine=engine, df=df, table_name=table_name, if_row_exists='update',
                      schema=schema, chunksize=chunksize, dtype=dtype, yield_chunks=True)

    for ix, result in enumerate(iterator):
        assert result.rowcount == (chunksize if ix != nb_chunks-1 else nb_last_chunk)

    # verify the inserted data is as expected
    # we sort the index for MySQL
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())
