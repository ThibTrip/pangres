## <span style="color:purple">pangres.adjust\_chunksize</span>_(con: sqlalchemy.engine.interfaces.Connectable, df: pandas.core.frame.DataFrame, chunksize: int)_

Checks if given `chunksize` is appropriate for upserting rows in given database using
given DataFrame.
The `chunksize` represents the number of rows you would like to upsert in a chunk when
using the `pangres.upsert` function.

Some databases have limitations on the number of SQL parameters and we need one parameter
per value for upserting data.
E.g. a DataFrame of 5 **columns+index levels** and 4 rows will require 5*4=20 SQL parameters.

This function will check the database type (e.g. SQlite) and the number of **columns+index levels**
to determine if the `chunksize` does not exceed limits and propose a lower one if it does.
Otherwise the same `chunksize` that you gave as input is returned.

This function currently takes into account max parameters limitations for the following cases:
* sqlite (32766 max for version >= 3.22.0 otherwise 999)
* asyncpg (32767 max)

If you know about more parameter limitations relevant for this library (PostgreSQL, MySQL, SQlite
or other databases I have not tested with this library that you managed to have working),
please contact me.

### Parameters

* **con**

  sqlalchemy Engine or Connection

* **df**

  DataFrame you would wish to upsert

* **chunksize**

  Size of chunks you would wish to use for upserting (represents the number of rows
  in each chunk)

### Raises

* **_TooManyColumnsForUpsertException_**

  When a DataFrame has more columns+index levels than the maximum number of allowed SQL variables
  for a SQL query for given database.
  In such a case even inserting row by row would not be possible because we would already
  have too many variables.
  For more information you can for instance google "SQLITE_MAX_VARIABLE_NUMBER".

### Examples

```python
from sqlalchemy import create_engine

# config (this assumes you have SQlite version >= 3.22.0)
engine = create_engine("sqlite://")

# some df we want to upsert
df = pd.DataFrame({'name':['Albert']}).rename_axis(index='profileid')
print(df.to_markdown())
```
|   profileid | name   |
|------------:|:-------|
|           0 | Albert |

```python
# adjust chunksize: 100,000 is too big of a chunksize in general for given database
# SQlite only allows 32766 parameters (values) at once maximum in a query
# since we have two columns (technically 1 column + 1 index level)
# we can only upsert in chunks of FLOOR(32766/2) rows maximum which is 16383
adjust_chunksize(con=engine, df=df, chunksize=100_000)
```
```python
16383
```