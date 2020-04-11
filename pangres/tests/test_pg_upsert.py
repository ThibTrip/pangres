import warnings
from pangres import pg_upsert, DocsExampleTable
from pangres.tests.conftest import drop_table_if_exists

# # Config

df = DocsExampleTable.df


# # Test

def test_deprecated_pg_upsert(engine, schema):
    table_name = 'test_pg_upsert'
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)

    if 'mysql' not in engine.dialect.dialect_description:
        # make sure we get a warning
        with warnings.catch_warnings(record=True) as w:
            pg_upsert(engine=engine, df=df, table_name=table_name, if_exists='upsert_overwrite', schema=schema)
            assert len(w) == 1
            assert issubclass(w[-1].category, FutureWarning)
            assert "deprecated" in str(w[-1].message)
        pg_upsert(engine=engine, df=df, table_name=table_name, if_exists='upsert_keep', schema=schema)
