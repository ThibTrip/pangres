import pandas as pd
import pytest
from pangres import upsert
from pangres.tests.conftest import AutoDropTableContext
from sqlalchemy.exc import OperationalError, ProgrammingError


# # Tests

def test_cannot_insert_missing_table_no_create(engine, schema):
    df = pd.DataFrame({'id':[0]}).set_index('id')
    with AutoDropTableContext(engine=engine, table_name='test_fail_missing_table') as ctx:
        with pytest.raises((OperationalError, ProgrammingError)) as excinfo:
            upsert(engine=engine, schema=schema, df=df, table_name=ctx.table_name,
                   if_row_exists='update', create_table=False)
            assert any(s in str(excinfo.value) for s in ('no such table', 'does not exist'))
