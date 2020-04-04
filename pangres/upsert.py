"""
Functions for preparing/compiling and executing upsert statements
in different SQL flavors.
"""
from copy import deepcopy
from sqlalchemy.sql.expression import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql.dml import insert as mysql_insert
from sqlalchemy.ext.compiler import compiles, deregister

# # Postgres

def postgres_upsert(engine, connection, table, values, if_row_exists):
    """
    Prepares and executes a PostgreSQL INSERT ON CONFLICT...DO NOTHING
    or DO UPDATE statement via sqlalchemy.dialects.postgresql.insert

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
    connection : sqlalchemy.engine.base.Connection
    table : sqlalchemy.sql.schema.Table
    values : list of dict
    if_row_exists : {'update', 'ignore'}
        * If 'update' issues a ON CONFLICT...DO UPDATE statement
        * If 'ignore' issues a ON CONFLICT...DO NOTHING statement
    """
    insert_stmt = pg_insert(table).values(values)
    if if_row_exists == 'ignore':
        upsert = insert_stmt.on_conflict_do_nothing()
    elif if_row_exists == 'update':
        update_cols = [c.name
                       for c in table.c
                       if c not in list(table.primary_key.columns)]
        upsert = insert_stmt.on_conflict_do_update(index_elements=table.primary_key.columns,
                                                   set_={k: getattr(insert_stmt.excluded, k)
                                                         for k in update_cols})        
    # execute upsert
    connection.execute(upsert)

# # MySQL

def mysql_upsert(engine, connection, table, values, if_row_exists):
    """
    Prepares and executes a MySQL INSERT IGNORE or
    INSERT...ON DUPLICATE KEY UPDATE
    via sqlalchemy.dialects.mysql.dml.insert

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
    connection : sqlalchemy.engine.base.Connection
    table : sqlalchemy.sql.schema.Table
    values : list of dict
    if_row_exists : {'update', 'ignore'}
        * If 'update' issues a ON DUPLICATE KEY UPDATE statement
        * If 'ignore' issues a INSERT IGNORE statement
    """
    insert_stmt = mysql_insert(table).values(values)
    if if_row_exists == 'ignore':
        # thanks to: https://stackoverflow.com/a/50870348/10551772
        upsert = insert_stmt.prefix_with('IGNORE')
    elif if_row_exists == 'update':
        # thanks to: https://stackoverflow.com/a/58180407/10551772
        # prepare kwargs for on_duplicated_key_update (with kwargs and getattr
        # even "bad" column names will resolve e.g. columns with spaces)
        update_cols = {}
        for col in insert_stmt.table.columns:
            col_name = col.name
            if col_name not in table.primary_key:
                update_cols.update({col_name:getattr(insert_stmt.inserted, col_name)})
        upsert = insert_stmt.on_duplicate_key_update(**update_cols)
    # execute upsert
    connection.execute(upsert)

# # Sqlite 
# (or other databases where ON CONFLICT...DO UPDATE SET/DO NOTHING is supported)

def sqlite_upsert(engine, connection, table, values, if_row_exists):
    """
    Compiles and executes a SQlite ON CONFLICT...DO NOTHING or DO UPDATE
    statement.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
    connection : sqlalchemy.engine.base.Connection
    table : sqlalchemy.sql.schema.Table
    values : list of dict
    if_row_exists : {'update', 'ignore'}
        * If 'update' issues a ON CONFLICT...DO UPDATE statement
        * If 'ignore' issues a ON CONFLICT...DO NOTHING statement

    Notes
    -----
    Compiling the SQL statement does not offer any significant
    performance improvement but allows for conversion of types such
    as dicts or lists to JSON sql type.
    """
    def escape_col(col):
        # unbound column from its table
        # otherwise the column would compile as "table.col_name"
        # which we could not use in e.g. SQlite
        unbound_col = deepcopy(col)
        unbound_col.table = None
        return str(unbound_col.compile(dialect=engine.dialect))

    # create custom SQL statement compiling
    from sqlalchemy.sql.expression import Insert
    @compiles(Insert)
    def compile_general_upsert(insert_stmt, compiler, **kwargs):
        # prepare start of insert (INSERT VALUES (...) ON CONFLICT)
        pk = [escape_col(c) for c in table.primary_key]
        insert = compiler.visit_insert(insert_stmt, **kwargs)
        ondup = f'ON CONFLICT ({",".join(pk)})'
        if if_row_exists == 'ignore':
            ondup_action = 'DO NOTHING'
            upsert = ' '.join((insert, ondup, ondup_action))
        elif if_row_exists == 'update':
            ondup_action = 'DO UPDATE SET'
            non_pks = [escape_col(c) for c in table.columns
                       if c not in list(table.primary_key)]
            updates = ', '.join(f'{c}=EXCLUDED.{c}' for c in non_pks)
            upsert = ' '.join((insert, ondup, ondup_action, updates))
        return upsert
    # use custom SQL statement compiling
    upsert = table.insert().values(values)
    connection.execute(upsert)
    # reset Insert to "normal"
    deregister(Insert)
