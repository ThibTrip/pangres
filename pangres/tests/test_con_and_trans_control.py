# +
"""
This module tests different ways of using pangres by providing a connection
instead of an engine and by using transactions
"""
import pandas as pd
import pytest
from sqlalchemy import text, VARCHAR

# local imports
from pangres import aupsert, upsert
from pangres.transaction import TransactionHandler
from pangres.tests.conftest import (commit, drop_table_for_test,
                                    select_table, TableNames, parameterize_async)


# -

# # Tests

# ## Reusing a connection

# +
@drop_table_for_test(TableNames.REUSE_CONNECTION)
def test_connection_usable_after_upsert(engine, schema):
    df = pd.DataFrame(index=pd.Index([0], name='ix'))
    with engine.connect() as con:
        # do some random upsert operation
        upsert(con=con, df=df, schema=schema,
               table_name=TableNames.REUSE_CONNECTION,
               if_row_exists='update')
        # attempt to reuse the connection
        result = con.execute(text('SELECT 1;')).scalar()
        assert result == 1
        commit(con)


# ASYNC VARIANT (identified by suffix `_async`, only executed for async engines)
@pytest.mark.asyncio
@drop_table_for_test(TableNames.REUSE_CONNECTION)
async def test_connection_usable_after_upsert_async(engine, schema):
    df = pd.DataFrame(index=pd.Index([0], name='ix'))
    async with engine.connect() as con:
        # do some random upsert operation
        await aupsert(con=con, df=df, schema=schema,
                      table_name=TableNames.REUSE_CONNECTION,
                      if_row_exists='update')
        # attempt to reuse the connection
        proxy = await con.execute(text('SELECT 1;'))
        result = proxy.scalar()
        assert result == 1
        await con.commit()


# -

# ## Using our own transaction

# +
@pytest.mark.parametrize("trans_op", ['commit', 'rollback'])
@drop_table_for_test(TableNames.COMMIT_OR_ROLLBACK_TRANS)
def test_transaction(engine, schema, trans_op):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_OR_ROLLBACK_TRANS
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix':VARCHAR(3)})

    with engine.connect() as con:
        trans = con.begin()
        try:
            # do some random upsert operation
            upsert(con=con, df=df, **common_kwargs)
            # do some other operation that requires commit
            upsert(con=con, df=df.rename(index={'foo':'bar'}), **common_kwargs)
            getattr(trans, trans_op)()  # commit or rollback
        finally:
            trans.close()

    # if trans_op=='commit': make sure we have "bar" and "foo" in the index
    # elif trans_op=='rollback': make sure we don't have any data
    # or that the table was not even created (what is rolled back
    # depends on the database type and other factors)
    if trans_op == 'commit':
        df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
        pd.testing.assert_frame_equal(df_db.sort_index(),
                                      pd.DataFrame(index=pd.Index(['bar', 'foo'], name='ix')))
    elif trans_op == 'rollback':
        df_db = select_table(engine=engine, schema=schema, table_name=table_name, error_if_missing=False)
        # no table or an empty table
        assert df_db is None or len(df_db) == 0


# ASYNC VARIANT (identified by suffix `_async`, only executed for async engines)
@pytest.mark.asyncio
@parameterize_async("trans_op", ['commit', 'rollback'])
@drop_table_for_test(TableNames.COMMIT_OR_ROLLBACK_TRANS)
async def test_transaction_async(engine, schema, trans_op=None):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_OR_ROLLBACK_TRANS
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix':VARCHAR(3)})

    async with engine.connect() as con:
        trans = await con.begin()
        try:
            # do some random upsert operation
            await aupsert(con=con, df=df, **common_kwargs)
            # do some other operation that requires commit
            await aupsert(con=con, df=df.rename(index={'foo':'bar'}), **common_kwargs)
            coro = getattr(trans, trans_op)  # commit or rollback
            await coro()
        finally:
            await trans.close()

    # if trans_op=='commit': make sure we have "bar" and "foo" in the index
    # elif trans_op=='rollback': make sure we don't have any data
    # or that the table was not even created (what is rolled back
    # depends on the database type and other factors)
    if trans_op == 'commit':
        df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
        pd.testing.assert_frame_equal(df_db.sort_index(),
                                      pd.DataFrame(index=pd.Index(['bar', 'foo'], name='ix')))
    elif trans_op == 'rollback':
        df_db = select_table(engine=engine, schema=schema, table_name=table_name, error_if_missing=False)
        # no table or an empty table
        assert df_db is None or len(df_db) == 0


# -

# ## Commit-as-you-go or ROLLBACK

# +
@drop_table_for_test(TableNames.COMMIT_AS_YOU_GO)
def test_commit_as_you_go(engine, schema):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_AS_YOU_GO
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix':VARCHAR(3)})

    with engine.connect() as con:
        # skip for sqlalchemy < 2.0 or when future=True flag is not passed
        # during engine creation (commit-as-you-go is a new feature)
        # when this is the case there is no attribute commit or rollback for
        # the connection
        if not hasattr(con, 'commit'):
            pytest.skip('test not possible because there is no attribute "commit" (most likely sqlalchemy < 2)')

        # do some random upsert operation and commit
        upsert(con=con, df=df, **common_kwargs)
        con.commit()

        # do some other operation that requires commit and then rollback
        upsert(con=con, df=df.rename(index={'foo':'bar'}), **common_kwargs)
        con.rollback()

    # the table in the db should be equal to the initial df as the second
    # operation was rolled back
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
    pd.testing.assert_frame_equal(df_db, df)


# ASYNC VARIANT (identified by suffix `_async`, only executed for async engines)
@pytest.mark.asyncio
@drop_table_for_test(TableNames.COMMIT_AS_YOU_GO)
async def test_commit_as_you_go_async(engine, schema):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_AS_YOU_GO
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix':VARCHAR(3)})

    async with engine.connect() as con:
        # skip for sqlalchemy < 2.0 or when future=True flag is not passed
        # during engine creation (commit-as-you-go is a new feature)
        # when this is the case there is no attribute commit or rollback for
        # the connection
        if not hasattr(con, 'commit'):
            pytest.skip('test not possible because there is no attribute "commit" (most likely sqlalchemy < 2)')

        # do some random upsert operation and commit
        await aupsert(con=con, df=df, **common_kwargs)
        await con.commit()

        # do some other operation that requires commit and then rollback
        await aupsert(con=con, df=df.rename(index={'foo':'bar'}), **common_kwargs)
        await con.rollback()

    # the table in the db should be equal to the initial df as the second
    # operation was rolled back
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
    pd.testing.assert_frame_equal(df_db, df)


# -

# # Test errors

def test_non_connectable_transaction_handler(_):
    with pytest.raises(TypeError) as exc_info:
        with TransactionHandler(connectable='abc'):
            pass
    assert 'sqlalchemy connectable' in str(exc_info)
