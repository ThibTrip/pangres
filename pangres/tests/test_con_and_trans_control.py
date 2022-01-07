"""
This module tests different ways of using pangres by providing a connection
instead of an engine and by using transactions
"""
import pandas as pd
import pytest
from sqlalchemy import text, VARCHAR
from pangres import upsert_future
from pangres.tests.conftest import commit, drop_table_for_test, TableNames


# # Tests

# +
@drop_table_for_test(TableNames.REUSE_CONNECTION)
def test_connection_usable_after_upsert(engine, schema):
    df = pd.DataFrame(index=pd.Index([0], name='ix'))
    with engine.connect() as con:
        # do some random upsert operation
        upsert_future(con=con, df=df, schema=schema,
                      table_name=TableNames.REUSE_CONNECTION,
                      if_row_exists='update')
        # attempt to reuse the connection
        result = con.execute(text('SELECT 1;')).scalar()
        assert result == 1
        commit(con)

@drop_table_for_test(TableNames.COMMIT_TRANS)
def test_transaction_commit_after_upsert(engine, schema):
    # todo: upsert, do a basic SQL operation (that requires commit) and commit
    pass
            
@drop_table_for_test(TableNames.ROLLBACK_TRANS)
def test_transaction_rollback_after_upsert(engine, schema):
    # todo: upsert, rollback and check that upsert was not executed
    pass

@drop_table_for_test(TableNames.COMMIT_AS_YOU_GO)
def test_commit_as_you_go(engine, schema):
    # todo: upsert, commit, upsert, rollback and check if only first upsert
    # is present
    pass
