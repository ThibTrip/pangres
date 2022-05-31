## <span style="color:purple">pangres.fix\_psycopg2\_bad\_cols</span>_(df: pandas.core.frame.DataFrame, replacements: dict = {'%': '', '(': '', ')': ''}) -> pandas.core.frame.DataFrame_

Replaces '%', '(' and ')' (characters that won't play nicely or even
at all with psycopg2) in column and index names in a deep copy of df.
This is a workaround for the unresolved issue
described here: https://github.com/psycopg/psycopg2/issues/167

**IMPORTANT**:
You will need to apply the same changes in the database as
well if the SQL table already exists for a given DataFrame.
Otherwise you will for instance end up with a column
"total_%" and "total_" in your SQL table.

### Parameters

* **df** : **_pd.DataFrame_**

* **replacements** : **_dict {'%':str, '(':str, ')':str}, default {'%':'', '(':'', ')':''}_**

  The keys '%', '(' and ')' are mandatory.
  There cannot be any extra keys.

### Returns

* **new\_df** : **_pd.DataFrame_**

### Raises

* **_pangres.exceptions.UnnamedIndexLevelsException_**

  When you pass a df where not all index levels are named

* **_pangres.exceptions.DuplicateLabelsException_**

  When you pass a df with duplicated labels accross index/columns
  or when after cleaning we end up with duplicated labels
  e.g. "test(" and "test)" would by default both be renamed to "test"

* **_TypeError_**

  When `replacements` is not of the expected type or has wrong keys
  or has non string values

### Examples

* fix bad col/index names with default replacements (empty string for '(', ')' and '%')
```python
from pangres import fix_psycopg2_bad_cols
import pandas as pd
df = pd.DataFrame({'test()':[0],
                   'foo()%':[0]}).set_index('test()')
print(df.to_markdown())
```
|   test() |   foo()% |
|---------:|---------:|
|        0 |        0 |

```python
df_fixed = fix_psycopg2_bad_cols(df)
print(df_fixed.to_markdown())
```
|   test |   foo |
|-------:|------:|
|      0 |     0 |

* fix bad col/index names with custom replacements - you MUST provide replacements for '(', ')' and '%'!
```python
import pandas as pd
df = pd.DataFrame({'test()':[0],
                   'foo()%':[0]}).set_index('test()')
print(df.to_markdown())
```
|   test() |   foo()% |
|---------:|---------:|
|        0 |        0 |

```python
df_fixed = fix_psycopg2_bad_cols(df, replacements={'%':'percent', '(':'', ')':''})
print(df_fixed.to_markdown())
```
|   test |   foopercent |
|-------:|-------------:|
|      0 |            0 |