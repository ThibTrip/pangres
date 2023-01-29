You can set the logging level of pangres by setting the environment variable `PANGRES_LOG_LEVEL` before the first call to one of `pangres`'s functions (ideally do it even before importing `pangres`).

```python
import logging
import os
os.environ['PANGRES_LOG_LEVEL'] = str(logging.WARNING)
from pangres import upsert

# now any logs under the WARNING level will be hidden when using pangres 
# upsert(...)
```