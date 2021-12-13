# +
import pandas as pd
import pytest
from loguru import logger
from sqlalchemy import create_engine, text

from pangres_imitation import upsert, FakeError, df_expected
# -

# # Config

table_name = 'test_transaction'


# # Tests

@pytest.mark.parametrize('connectable_type', ('connection', 'engine'), scope='session')
@pytest.mark.parametrize('use_own_transaction', (True, False), ids=('own_transaction', 'pangres_transaction'), scope='session')
@pytest.mark.parametrize('yield_chunks', (False, True), ids=('yield_chunks', 'no_yield_chunks'), scope='session')
def test_upsert(conn_string, future, # global parameters
                connectable_type, use_own_transaction, yield_chunks, simulate_error=False):
    # get connectable
    kwargs = {'future':future} if future else {}
    engine = create_engine(conn_string, **kwargs)
    with engine.connect() as temp_con:
        temp_con.execute(text(f'DROP TABLE IF EXISTS {table_name};'))
        if hasattr(temp_con, 'commit'):
            temp_con.commit()

    if connectable_type == 'connection':
        connectable = engine.connect()
    else:
        connectable = engine
    if use_own_transaction:
        trans = connectable.begin()
    else:
        trans = None

    # upsert at once...
    try:
        results = upsert(con=connectable, table_name=table_name, yield_chunks=yield_chunks,
                         future=future, simulate_error=simulate_error,
                         mysql_like=conn_string.startswith('mysql'))
    except Exception as e:
        if simulate_error and isinstance(e, FakeError):
            logger.error(e)
            return
        else:
            raise e

    # ...or upsert with generator
    if yield_chunks:
        for r in results:
            print(f'{r.rowcount} rows updated')

    # commit
    if hasattr(trans, 'commit'):
        trans.commit()

    # verify data integrity
    with engine.connect() as temp_con:
        results_db = temp_con.execute(text(f'SELECT * FROM {table_name}')).fetchall()
        df_db = pd.DataFrame(results_db, columns=['id', 'name'])
        print(df_db.to_markdown())
        pd.testing.assert_frame_equal(df_expected, df_db)

    # verify resources status
    if connectable_type == 'connection':
        assert not connectable.closed
