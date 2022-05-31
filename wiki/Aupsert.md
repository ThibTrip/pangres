## <span style="color:purple">pangres.aupsert</span>_(con, df: pandas.core.frame.DataFrame, table\_name: str, if\_row\_exists: str, schema: Optional[str] = None, create\_schema: bool = False, create\_table: bool = True, add\_new\_columns: bool = False, adapt\_dtype\_of\_empty\_db\_columns: bool = False, chunksize: Optional[int] = None, dtype: Optional[dict] = None, yield\_chunks: bool = False)_

Asynchronous variant of `pangres.upsert`. Make sure to read its docstring
before using this function!

The parameters of `pangres.aupsert` are the same but parameter `con`
will require an asynchronous connectable (asynchronous engine or asynchronous connection).

For example you can use PostgreSQL asynchronously with `sqlalchemy` thanks to
the library/driver `asyncpg`, or SQLite with `aiosqlite` or Mysql with `aiomysql`.

**WARNING**

Although this has been well tested in `pangres` you should read this warning from
`sqlalchemy` (underlying library we use):

> The asyncio extension as of SQLAlchemy 1.4.3 can now be considered to be beta level software.
> API details are subject to change however at this point it is unlikely for
> there to be significant backwards-incompatible changes.
>
> Source: https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html

**IMPORTANT NOTES ON CONNECTIONS AND TRANSACTIONS**:

The notes on transactions in the docstring `pangres.upsert` apply here as well
but async connections do **not autocommit** even prior to sqlalchemy 2.0.
This means that if you are passing an asynchronous connection you will have to commit yourself
(manually via the connection or a transaction or context managers..., see Examples below).

**IMPORTANT info for IPYTHON/JUPYTER users**

You will need to run the following code before executing asynchronous code in an IPython context
(this includes Jupyter notebooks and also inside of Jupyter Lab). This is not specific to `pangres`.

```
import nest_asyncio  # pip install nest_asyncio
nest_asyncio.apply()
```

Not doing this could result in the following exception occuring:

```
RuntimeError: asyncio.run() cannot be called from a running event loop
```

**IMPORTANT info about parallelism**

`aupsert` is subject to race conditions due to its asynchronous nature. For instance,
imagine the following scenario:

1. 2 coroutines (parallel functions in Python) A and B both do an upsert operation
2. A and B check if the table does not exist at roughly the same time
3. The database tells both coroutines that the table does not exist
4. Both coroutines try to create the table at roughly the same time:
   one coroutine should succeed but the other one will raise an Exception

For examples of what kind of race conditions can occur, see this notebook:
https://github.com/ThibTrip/pangres/blob/master/demos/gotchas_asynchronous_pangres.ipynb

### Examples

```python
import asyncio
import pandas as pd
from pangres import aupsert, DocsExampleTable
from sqlalchemy.ext.asyncio import create_async_engine # doctest: +SKIP

# config
engine = create_async_engine("postgresql+asyncpg://username:password@localhost:5432/postgres") # doctest: +SKIP

# get some data
df = DocsExampleTable.df

# Create table before inserting! This will avoid race conditions mentionned above
# (here we are lazy so we'll use pangres to do that but we could also use a sqlalchemy ORM model)
# By using `df.head(0)` we get 0 rows but we have all the information about columns, index levels
# and data types that we need for creating the table.
# And in a second step (see coroutine `execute_upsert` that we define after)
# we will set all parameters that could cause structure changes
# to False so we can run queries in parallel without worries!
async def setup():
    await aupsert(con=engine, df=df.head(0),
                  table_name='example',
                  if_row_exists='update',
                  create_schema=True,
                  create_table=True,
                  add_new_columns=True)
asyncio.run(setup()) # doctest: +SKIP

# now that we know the table exists, let's insert data into it
# the example is a bit stupid since we only have one coroutine but
# you should get the idea.
# See variable `coroutines` below where we could add several coroutines
# in order to make queries in parallel!)
async def execute_upsert():
    async with engine.connect() as connection:
        await aupsert(con=connection, df=df,
                      table_name='example',
                      if_row_exists='update',
                      # set this to False (other structure related parameters are False by default)
                      create_table=False)
        await connection.commit()  # !IMPORTANT
loop = asyncio.get_event_loop() # doctest: +SKIP
coroutines = [execute_upsert()] # doctest: +SKIP
tasks = asyncio.gather(*coroutines) # doctest: +SKIP
loop.run_until_complete(tasks) # doctest: +SKIP
```

* alternative to the coroutine above but iterating over results of inserted chunks
  so that we can for instance get the number of updated rows
```python
async def execute_upsert():
    async with engine.connect() as connection:
        # this creates an async generator
        async_gen = await aupsert(con=connection, df=df,
                                  table_name='example',
                                  if_row_exists='update',
                                  yield_chunks=True,  # <--- WE SET THIS TO TRUE for iterating
                                  # set this to False (other structure related parameters are False by default)
                                  create_table=False)
        async for result in async_gen:
            print(f'{result.rowcount} updated rows')  # print the number of updated rows

        # !IMPORTANT (note that you could also commit between chunks if that makes any sense in your case)
        await connection.commit()
```