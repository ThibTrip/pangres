#!/usr/bin/env python
# coding: utf-8
"""
Async specific tests
"""
import asyncio
import math
import pandas as pd
import pytest
from sqlalchemy import VARCHAR
from pangres import aupsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import ReaderSQLExampleTables, adrop_table_if_exists

# # Check if we can run two parallel upsert commands at once even though they try to create a table

def test_no_conflict_table_creation(engine, schema):
    table_name = 'test_async_no_conflict_table_creation'

    # drop table if exists
    loop = asyncio.get_event_loop()
    coroutines = [adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)]
    tasks = asyncio.gather(*coroutines)
    loop.run_until_complete(tasks)

    # the 2 coroutines below are both going to try to create the table.
    # As the table does not exist yet we check if there is any
    # race condition problem here
    df = pd.DataFrame({'id':[1], 'value':[100]}).set_index('id')
    kwargs = dict(engine=engine, schema=schema, table_name=table_name,
                  df=df, create_table=True, if_row_exists='update')
    coroutines = [aupsert(**kwargs), aupsert(**kwargs)]
    tasks = asyncio.gather(*coroutines, return_exceptions=True)
    results = loop.run_until_complete(tasks)
    for r in results:
        if isinstance(r, Exception):
            raise r
