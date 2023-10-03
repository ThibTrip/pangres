Welcome to the pangres wiki!

# Quickstart

Install `pangres` with:

```bash
pip install pangres
```

Here is a basic usage example (see more in the sections below):

```python
import pandas as pd
from pangres import upsert
from sqlalchemy import create_engine

# create or get a pandas DataFrame
data = {'id': [10],
        'name': ['Albert']}
df = pd.DataFrame(data)
df = df.set_index('id')  # a unique index is required!

# get a SQL engine from sqlalchemy (used to connect to the SQL database)
engine = create_engine('sqlite://')
table_name = 'test'

# update given SQL table using the DataFrame
upsert(
    con=engine,
    df=df,
    table_name=table_name,
    if_row_exists='update',
    dtype=None,  # same logic as the parameter in pandas.to_sql
    chunksize=1000,
    create_table=True  # create a new table if it does not exist
)
```

# Usage

[Upserting DataFrames in SQL](https://github.com/ThibTrip/pangres/wiki/Upsert)

[Upserting DataFrames in SQL asynchronously](https://github.com/ThibTrip/pangres/wiki/Aupsert)

[Checking and adjusting the size of chunks to upsert](https://github.com/ThibTrip/pangres/wiki/Chunksize-Adjustment)

[Fix bad column names for Postgres](https://github.com/ThibTrip/pangres/wiki/Fix-bad-column-names-postgres)

[Logging](https://github.com/ThibTrip/pangres/wiki/Logging)

See also **demo notebooks** below.

# Demo notebooks

[Demo notebook for `pangres.upsert`](https://github.com/ThibTrip/pangres/blob/master/demos/pangres_demo.ipynb)

[Demo notebook for `pangres.upsert` with a progress bar](https://github.com/ThibTrip/pangres/blob/master/demos/pangres_demo_tqdm.ipynb)

[Notebook with transaction control and commit-as-you-go workflows example](https://github.com/ThibTrip/pangres/blob/master/demos/transaction_control.ipynb)

[Gotchas with asynchronous upserts (`pangres.aupsert`)](https://github.com/ThibTrip/pangres/blob/master/demos/gotchas_asynchronous_pangres.ipynb)

# Notes

Parts of the documentation were automatically generated using the docstrings of pangres' functions/classes/methods via my library [npdoc_to_md](https://github.com/ThibTrip/npdoc_to_md).
