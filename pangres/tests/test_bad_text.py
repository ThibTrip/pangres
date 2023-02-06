# -*- coding: utf-8 -*-
# +
import pandas as pd
import pytest
import random

# local imports
from pangres import aupsert, upsert, fix_psycopg2_bad_cols
from pangres.exceptions import BadColumnNamesException
from pangres.tests.conftest import (adrop_table_between_tests, drop_table_between_tests,
                                    sync_or_async_test, TableNames)
# -

# # Helpers

# list of characters that could potentially cause errors in column names or even values
bad_char_seq = """/_- ?§$&"',:;*()%[]{}|<>=!+#""" + "\\"


# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# ## Insert bad text

# +
@drop_table_between_tests(table_name=TableNames.BAD_TEXT)
def run_test_bad_text_insert(engine, schema):
    # add bad text in a column named 'text'
    create_random_text = lambda: ''.join(random.choice(bad_char_seq) for i in range(10))
    df_test = (pd.DataFrame({'text': [create_random_text() for i in range(10)]})
               .rename_axis(['profileid'], axis='index', inplace=False))
    upsert(con=engine, schema=schema, table_name=TableNames.BAD_TEXT, df=df_test, if_row_exists='update')


@adrop_table_between_tests(table_name=TableNames.BAD_TEXT)
async def run_test_bad_text_insert_async(engine, schema):
    # add bad text in a column named 'text'
    create_random_text = lambda: ''.join(random.choice(bad_char_seq) for i in range(10))
    df_test = (pd.DataFrame({'text': [create_random_text() for i in range(10)]})
               .rename_axis(['profileid'], axis='index', inplace=False))
    await aupsert(con=engine, schema=schema, table_name=TableNames.BAD_TEXT, df=df_test, if_row_exists='update')


# -

# ## Add colums with bad names

# +
@drop_table_between_tests(table_name=TableNames.BAD_COLUMN_NAMES)
def run_test_bad_column_names(engine, schema, iteration):
    # add columns with bad names
    # don't do this for MySQL which has more strict rules for column names
    if 'mysql' in engine.dialect.dialect_description:
        pytest.skip('MySQL has very strict rules for column names so we do not even test it')

    random_bad_col_name = ''.join(random.choice(bad_char_seq) for i in range(50))
    df_test = (pd.DataFrame({random_bad_col_name: ['test', None]})
               .rename_axis(['profileid'], axis='index', inplace=False))

    # psycopg2 can't process columns with "%" or "(" or ")" so we will need `fix_psycopg2_bad_cols`
    if 'postgres' in engine.dialect.dialect_description:
        df_test = fix_psycopg2_bad_cols(df_test)
    upsert(con=engine, schema=schema, df=df_test, table_name=TableNames.BAD_COLUMN_NAMES, if_row_exists='update')


@adrop_table_between_tests(table_name=TableNames.BAD_COLUMN_NAMES)
async def run_test_bad_column_names_async(engine, schema, iteration):
    # add columns with bad names
    # don't do this for MySQL which has more strict rules for column names
    if 'mysql' in engine.dialect.dialect_description:
        pytest.skip('MySQL has very strict rules for column names so we do not even test it')

    random_bad_col_name = ''.join(random.choice(bad_char_seq) for i in range(50))
    df_test = (pd.DataFrame({random_bad_col_name: ['test', None]})
               .rename_axis(['profileid'], axis='index', inplace=False))

    # psycopg2 can't process columns with "%" or "(" or ")" so we will need `fix_psycopg2_bad_cols`
    if 'postgres' in engine.dialect.dialect_description:
        df_test = fix_psycopg2_bad_cols(df_test)
    await aupsert(con=engine, schema=schema, df=df_test, table_name=TableNames.BAD_COLUMN_NAMES,
                  if_row_exists='update')


# -

# ## Add colums with bad names (PostgreSQL)

# +
@drop_table_between_tests(table_name=TableNames.BAD_COLUMN_NAMES_PG)
def run_test_bad_column_name_postgres_raises(engine, schema):
    if 'postgres' not in engine.dialect.dialect_description:
        pytest.skip('This test is only relevant for PostgreSQL')
    df = pd.DataFrame({'id': [0], '(test)': [0]}).set_index('id')
    with pytest.raises(BadColumnNamesException) as exc_info:
        upsert(con=engine, schema=schema, df=df, table_name=TableNames.BAD_COLUMN_NAMES_PG,
               if_row_exists='update')
    assert 'does not seem to support column names with' in str(exc_info.value)


@adrop_table_between_tests(table_name=TableNames.BAD_COLUMN_NAMES_PG)
async def run_test_bad_column_name_postgres_raises_async(engine, schema):
    if 'postgres' not in engine.dialect.dialect_description:
        pytest.skip('This test is only relevant for PostgreSQL')
    df = pd.DataFrame({'id': [0], '(test)': [0]}).set_index('id')
    with pytest.raises(BadColumnNamesException) as exc_info:
        await aupsert(con=engine, schema=schema, df=df, table_name=TableNames.BAD_COLUMN_NAMES_PG,
                      if_row_exists='update')
    assert 'does not seem to support column names with' in str(exc_info.value)


# -

# ## Another test with the column name `values` (see issue #34 of pangres)

# +
@drop_table_between_tests(table_name=TableNames.COLUMN_NAMED_VALUES)
def run_test_column_named_values(engine, schema):
    df = pd.DataFrame({'values': range(5, 9)}, index=pd.Index(range(1, 5), name='idx'))
    upsert(con=engine, schema=schema, df=df, if_row_exists='update',
           table_name=TableNames.COLUMN_NAMED_VALUES)


@adrop_table_between_tests(table_name=TableNames.COLUMN_NAMED_VALUES)
async def run_test_column_named_values_async(engine, schema):
    df = pd.DataFrame({'values': range(5, 9)}, index=pd.Index(range(1, 5), name='idx'))
    await aupsert(con=engine, schema=schema, df=df, if_row_exists='update',
                  table_name=TableNames.COLUMN_NAMED_VALUES)


# -

# # Actual tests

# +
def test_bad_text_insert(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_bad_text_insert_async,
                       f_sync=run_test_bad_text_insert)


# do the next test multiple times to try different combinations of bad characters
@pytest.mark.parametrize('iteration', range(5), ids=[f'iteration{i}' for i in range(5)])
def test_bad_column_names(engine, schema, iteration):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_bad_column_names_async,
                       f_sync=run_test_bad_column_names,
                       iteration=iteration)


def test_bad_column_name_postgres_raises(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_bad_column_name_postgres_raises_async,
                       f_sync=run_test_bad_column_name_postgres_raises)


def test_column_named_values(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_column_named_values_async,
                       f_sync=run_test_column_named_values)
