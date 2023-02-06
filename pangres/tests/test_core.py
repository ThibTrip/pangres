#!/usr/bin/env python
# coding: utf-8
# +
"""
End to end test similar to the scenario proposed in the docstring
of `pangres.upsert_or_aupsert`.

We will create a table and then insert with update and then ignore for the
`ON CONFLICT` clause.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, VARCHAR
from sqlalchemy.exc import OperationalError, ProgrammingError

# local imports
from pangres import aupsert, upsert, HasNoSchemaSystemException
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import (adrop_schema, adrop_table, adrop_table_between_tests, aselect_table,
                                    drop_schema, drop_table, drop_table_between_tests,
                                    schema_for_testing_creation, select_table,
                                    sync_async_exec_switch, sync_or_async_test, TableNames)
# -


# # Helpers

# ## Test data

# +
# df from which we will create a SQL table
df = _TestsExampleTable.create_example_df(nb_rows=6)
# test nulls won't create any problems
# avoid nulls in boolean column though as this is unpractical with pandas in older versions
df.iloc[0, [ix for ix, col in enumerate(df.columns) if col != 'likes_pizza']] = None

# df from which we will upsert_or_aupsert update or upsert_or_aupsert ignore
# remove one record from above and add one
# so we know that 1) old records are still there, 2) new ones get added
# 3) we can check whether the update/ignore of existing records worked
df2 = _TestsExampleTable.create_example_df(nb_rows=7)
df2 = df2.iloc[1:]
# -

# ## Expectations

# +
df_after_insert_update = pd.concat(objs=(df.loc[~df.index.isin(df2.index.tolist())],  # everything that is not in df2
                                         df2))

df_after_insert_ignore = pd.concat(objs=(df,
                                         df2.loc[~df2.index.isin(df.index.tolist())]))  # everything that is not in df


# -

# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# ## Upsert without DDL modifications (except `create_table`)

# +
@drop_table_between_tests(table_name=TableNames.END_TO_END)
def run_test_end_to_end(engine, schema, create_table, if_row_exists, df_expected):
    # config
    table_name = TableNames.END_TO_END
    common_kwargs_upsert = dict(if_row_exists=if_row_exists, table_name=table_name)
    read_table = lambda: _TestsExampleTable.read_from_db(engine=engine, schema=schema,
                                                         table_name=table_name).sort_index()

    # 1. create table
    upsert(con=engine, schema=schema, df=df, create_table=True, **common_kwargs_upsert)
    pd.testing.assert_frame_equal(df, read_table())

    # 2. insert update/ignore
    upsert(con=engine, schema=schema, df=df2, create_table=create_table, **common_kwargs_upsert)
    pd.testing.assert_frame_equal(df_expected, read_table())


@adrop_table_between_tests(table_name=TableNames.END_TO_END)
async def run_test_end_to_end_async(engine, schema, create_table, if_row_exists, df_expected):
    # config
    table_name = TableNames.END_TO_END
    common_kwargs_upsert = dict(if_row_exists=if_row_exists, table_name=table_name)

    async def read_table():
        temp_df = await _TestsExampleTable.aread_from_db(engine=engine, schema=schema, table_name=table_name)
        return temp_df.sort_index()

    # 1. create table
    await aupsert(con=engine, schema=schema, df=df, create_table=True, **common_kwargs_upsert)
    df_db = await read_table()
    pd.testing.assert_frame_equal(df, df_db)

    # 2. insert update/ignore
    await aupsert(con=engine, schema=schema, df=df2, create_table=create_table, **common_kwargs_upsert)
    df_db = await read_table()
    pd.testing.assert_frame_equal(df_expected, df_db)


# -

# ## Upsert with addition of new columns

# +
@drop_table_between_tests(table_name=TableNames.ADD_NEW_COLUMN)
def run_test_add_new_column(engine, schema):
    # config
    table_name = TableNames.ADD_NEW_COLUMN
    dtype = {'id': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id': ['foo']}).set_index('id')
    # common kwargs for all the times we use upsert_or_aupsert
    common_kwargs = dict(con=engine, schema=schema, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    upsert(df=df, **common_kwargs)
    # 2. add a new column and repeat upsert_or_aupsert
    df['new_column'] = 'bar'
    upsert(df=df, add_new_columns=True, **common_kwargs)
    # verify content matches
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='id').sort_index()
    pd.testing.assert_frame_equal(df, df_db)


@adrop_table_between_tests(table_name=TableNames.ADD_NEW_COLUMN)
async def run_test_add_new_column_async(engine, schema):
    # config
    table_name = TableNames.ADD_NEW_COLUMN
    dtype = {'id': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id': ['foo']}).set_index('id')
    # common kwargs for all the times we use upsert_or_aupsert
    common_kwargs = dict(con=engine, schema=schema, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    await aupsert(df=df, **common_kwargs)
    # 2. add a new column and repeat upsert_or_aupsert
    df['new_column'] = 'bar'
    await aupsert(df=df, add_new_columns=True, **common_kwargs)
    # verify content matches
    df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name, index_col='id')
    pd.testing.assert_frame_equal(df, df_db.sort_index())


# -

# ## Upsert with alteration of data type for empty columns

# +
@drop_table_between_tests(table_name=TableNames.CHANGE_EMPTY_COL_TYPE)
def run_test_adapt_column_type(engine, schema):
    # skip for sqlite as it does not support such alteration
    if 'sqlite' in engine.dialect.dialect_description:
        pytest.skip('such column alteration is not possible with SQlite')

    # config
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    dtype = {'id': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id': ['foo'], 'empty_column': [None]}).set_index('id')
    # common kwargs for all the times we use upsert_or_aupsert
    common_kwargs = dict(con=engine, schema=schema, df=df, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    upsert(**common_kwargs)
    # 2. add non string data in empty column and repeat upsert_or_aupsert
    df['empty_column'] = 1
    upsert(**common_kwargs, adapt_dtype_of_empty_db_columns=True)


@adrop_table_between_tests(table_name=TableNames.CHANGE_EMPTY_COL_TYPE)
async def run_test_adapt_column_type_async(engine, schema):
    # skip for sqlite as it does not support such alteration
    if 'sqlite' in engine.dialect.dialect_description:
        pytest.skip('such column alteration is not possible with SQlite')

    # config
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    dtype = {'id': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id': ['foo'], 'empty_column': [None]}).set_index('id')
    # common kwargs for all the times we use upsert_or_aupsert
    common_kwargs = dict(con=engine, schema=schema, df=df, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    await aupsert(**common_kwargs)
    # 2. add non string data in empty column and repeat upsert_or_aupsert
    df['empty_column'] = 1
    await aupsert(**common_kwargs, adapt_dtype_of_empty_db_columns=True)


# -

# ## Upsert with `create_schema=True` but `schema=None`

# +
@drop_table_between_tests(table_name=TableNames.CREATE_SCHEMA_NONE)
def run_test_create_schema_none(engine, schema):
    df = pd.DataFrame({'id': [0]}).set_index('id')
    upsert(con=engine, schema=None, df=df, if_row_exists='update', create_schema=True,
           table_name=TableNames.CREATE_SCHEMA_NONE, create_table=True)


@adrop_table_between_tests(table_name=TableNames.CREATE_SCHEMA_NONE)
async def run_test_create_schema_none_async(engine, schema):
    df = pd.DataFrame({'id': [0]}).set_index('id')
    await aupsert(con=engine, schema=None, df=df, if_row_exists='update', create_schema=True,
                  table_name=TableNames.CREATE_SCHEMA_NONE, create_table=True)


# -

# ## Upsert with `create_schema=True` but `schema` is not None

# +
@drop_table_between_tests(table_name=TableNames.CREATE_SCHEMA_NOT_NONE)
def run_test_create_schema_not_none(engine, schema):
    # local helpers
    is_postgres = 'postgres' in engine.dialect.dialect_description

    # overwrite schema
    schema = schema_for_testing_creation

    # config
    df = pd.DataFrame({'id': [0]}).set_index('id')
    table_name = TableNames.CREATE_SCHEMA_NOT_NONE

    # drop table before test (could not get my decorator to work with another schema
    # when having an optional arg schema=None due to variable scopes problems)
    if is_postgres:
        drop_table(engine=engine, schema=schema, table_name=table_name)
        drop_schema(engine=engine, schema=schema)

    try:
        upsert(con=engine, schema=schema, df=df, if_row_exists='update', create_schema=True,
               table_name=table_name, create_table=True)
        if not is_postgres:  # pragma: no cover
            raise AssertionError('Expected `upsert` to fail when trying to create a schema '
                                 'with another database than postgres')
    except Exception as e:
        # for postgres this should have worked
        if is_postgres:  # pragma: no cover
            raise e
        else:
            assert isinstance(e, HasNoSchemaSystemException)
    finally:
        # drop table and schema after test
        if is_postgres:
            drop_table(engine=engine, schema=schema, table_name=table_name)
            drop_schema(engine=engine, schema=schema)


@adrop_table_between_tests(table_name=TableNames.CREATE_SCHEMA_NOT_NONE)
async def run_test_create_schema_not_none_async(engine, schema):
    # local helpers
    is_postgres = 'postgres' in engine.dialect.dialect_description

    # overwrite schema
    schema = schema_for_testing_creation

    # config
    df = pd.DataFrame({'id': [0]}).set_index('id')
    table_name = TableNames.CREATE_SCHEMA_NOT_NONE

    # drop table before test (could not get my decorator to work with another schema
    # when having an optional arg schema=None due to variable scopes problems)
    if is_postgres:
        await adrop_table(engine=engine, schema=schema, table_name=table_name)
        await adrop_schema(engine=engine, schema=schema)

    try:
        await aupsert(con=engine, schema=schema, df=df, if_row_exists='update', create_schema=True,
                      table_name=table_name, create_table=True)
        if not is_postgres:  # pragma: no cover
            raise AssertionError('Expected `upsert` to fail when trying to create a schema '
                                 'with another database than postgres')
    except Exception as e:
        # for postgres this should have worked
        if is_postgres:  # pragma: no cover
            raise e
        else:
            assert isinstance(e, HasNoSchemaSystemException)
    finally:
        # drop table and schema after test
        if is_postgres:
            await adrop_table(engine=engine, schema=schema, table_name=table_name)
            await adrop_schema(engine=engine, schema=schema)


# -

# ## Test error on upsert when table does not exist (with `create_table=False`)
#
# We do not create any table here

# +
def run_test_insert_missing_table(engine, schema):
    """
    Check if an error is raised when trying to insert in a missing table
    and `create_table` is False.
    """
    df = pd.DataFrame({'id': [0]}).set_index('id')
    with pytest.raises((OperationalError, ProgrammingError)) as excinfo:
        upsert(con=engine, schema=schema, df=df, table_name=TableNames.NO_TABLE,
               if_row_exists='update', create_table=False)
    assert any(s in str(excinfo.value) for s in ('no such table', 'does not exist', "doesn't exist"))


async def run_test_insert_missing_table_async(engine, schema):
    """
    Check if an error is raised when trying to insert in a missing table
    and `create_table` is False.
    """
    df = pd.DataFrame({'id': [0]}).set_index('id')
    with pytest.raises((OperationalError, ProgrammingError)) as excinfo:
        await aupsert(con=engine, schema=schema, df=df, table_name=TableNames.NO_TABLE,
                      if_row_exists='update', create_table=False)
    assert any(s in str(excinfo.value) for s in ('no such table', 'does not exist', "doesn't exist"))


# -

# ## Test if MySQL does not automatically create an autoincremented PK when giving it integers
#
# See [issue 56](https://github.com/ThibTrip/pangres/issues/56)

# +
@drop_table_between_tests(table_name=TableNames.PK_MYSQL)
def run_test_mysql_pk_not_auto_incremented(engine, schema):
    if 'mysql' not in engine.dialect.dialect_description:
        pytest.skip('This test is only relevant for MySQL')

    table_name = TableNames.PK_MYSQL

    # upsert first df using pangres which creates the table automatically
    df1 = pd.DataFrame({'id': [0, 1], 'name': ['foo', 'bar']}).set_index('id')
    upsert(con=engine, df=df1, table_name=table_name, if_row_exists='update')

    # upsert second df
    df2 = pd.DataFrame({'id': [100, 200], 'name': ['baz', 'qux']}).set_index('id')
    upsert(con=engine, df=df2, table_name=table_name, if_row_exists='update')

    # read df back
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='id')

    # check mysql got that correctly
    pd.testing.assert_frame_equal(df_db.sort_index(), pd.concat((df1, df2)).sort_index())


@adrop_table_between_tests(table_name=TableNames.PK_MYSQL)
async def run_test_mysql_pk_not_auto_incremented_async(engine, schema):
    if 'mysql' not in engine.dialect.dialect_description:
        pytest.skip('This test is only relevant for MySQL')

    table_name = TableNames.PK_MYSQL

    # upsert first df using pangres which creates the table automatically
    df1 = pd.DataFrame({'id': [0, 1], 'name': ['foo', 'bar']}).set_index('id')
    await aupsert(con=engine, df=df1, table_name=table_name, if_row_exists='update')

    # upsert second df
    df2 = pd.DataFrame({'id': [100, 200], 'name': ['baz', 'qux']}).set_index('id')
    await aupsert(con=engine, df=df2, table_name=table_name, if_row_exists='update')

    # read df back
    df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name, index_col='id')

    # check mysql got that correctly
    pd.testing.assert_frame_equal(df_db.sort_index(), pd.concat((df1, df2)).sort_index())


# -

# # Actual tests

# +
# after the table is created with a first `upsert_or_aupsert`
# using `create_table=False` or `create_table=True` should both work
@pytest.mark.parametrize('create_table', [False, True], ids=['create_table_false', 'create_table_true'])
@pytest.mark.parametrize('if_row_exists, df_expected', [['update', df_after_insert_update],
                                                        ['ignore', df_after_insert_ignore]],
                         ids=['update', 'ignore'])
def test_end_to_end(engine, schema, create_table, if_row_exists, df_expected):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_end_to_end_async,
                       f_sync=run_test_end_to_end,
                       create_table=create_table,
                       if_row_exists=if_row_exists,
                       df_expected=df_expected)


@pytest.mark.parametrize('use_async', [False, True], ids=['upsert', 'aupsert'])
def test_bad_value_if_row_exists(_, use_async):
    df = pd.DataFrame({'id': [0]}).set_index('id')
    engine = create_engine('sqlite:///')
    upsert_func = upsert if use_async else aupsert
    upsert_kwargs = dict(con=engine, df=df, table_name=TableNames.NO_TABLE, if_row_exists='test')
    with pytest.raises(ValueError) as excinfo:
        sync_async_exec_switch(upsert_func, **upsert_kwargs)
    assert 'must be "ignore" or "update"' in str(excinfo.value)


def test_add_new_column(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_add_new_column_async,
                       f_sync=run_test_add_new_column)


def test_adapt_column_type(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_adapt_column_type_async,
                       f_sync=run_test_adapt_column_type)


def test_insert_missing_table(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_insert_missing_table_async,
                       f_sync=run_test_insert_missing_table)


def test_create_schema_none(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_create_schema_none_async,
                       f_sync=run_test_create_schema_none)


def test_create_schema_not_none(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_create_schema_not_none_async,
                       f_sync=run_test_create_schema_not_none)


def test_mysql_pk_not_auto_incremented(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_mysql_pk_not_auto_incremented_async,
                       f_sync=run_test_mysql_pk_not_auto_incremented)
