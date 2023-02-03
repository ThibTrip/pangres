# +
class HasNoSchemaSystemException(Exception):
    """
    Raised when trying or preparing to execute operations
    related to SQL schemas on a non-compatible database
    (AFAIK schemas are a postgreSQL only feature)

    E.g. in `pangres.upsert` when `create_schema=True`
    and the database is a SQLite database.
    """
    pass


class BadColumnNamesException(Exception):
    """
    Raised when a user passes a DataFrame with column
    names incompatible with the SQL driver.

    E.g. psycopg2 (driver for PostgreSQL) cannot insert
    columns with parentheses in the name
    See https://github.com/psycopg/psycopg2/issues/167
    """
    pass


class UnnamedIndexLevelsException(Exception):
    """
    Raised in `pangres.upsert` when the user
    passes a DataFrame where some index levels
    have no names
    """
    pass


class DuplicateValuesInIndexException(Exception):
    """
    Raised in `pangres.upsert` when the user
    passes a DataFrame where some index entries
    are duplicated
    """
    pass


class DuplicateLabelsException(Exception):
    """
    Raised in `pangres.upsert` when the user
    passes a DataFrame where a name appears more than once
    across columns and index levels
    """
    pass


class MissingIndexLevelInSqlException(Exception):
    """
    Raised when the user asks to add a column into a SQL table
    but the column is part of the index we are supposed to use
    to upsert data
    """
    pass


class TooManyColumnsForUpsertException(Exception):
    """
    Raised when a DataFrame has so many columns, that an upsert
    operation of even one row cannot succeed due to limitations
    in the maximum number of SQL parameters of given database
    """
    pass
