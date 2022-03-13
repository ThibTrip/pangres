# Changelog

## [v4.1.1](https://github.com/ThibTrip/pangres/releases/tag/v4.1.1) (2022-03-13)


**Release Notes**


___
**_Bug Fixes_**

* fixed bug where I used a synchronous method instead of its asynchronous variant (`UpsertQuery.execute` instead of `UpsertQuery.aexecute` in `pangres.aupsert`). This has no repercussions for the end user

**_Documentation_**

* fix illogic code in example for `pangres.aupsert` (using `engine` instead of `connection` in contexts) and `commit` which I had forgotten!
* added changelog

**_Testing_**

* overhaul of the tests. asynchronous and synchronous tests have been separated
* module `test_upsert_end_to_end` has been renamed to `test_core`
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v4.1...v4.1.1)

**Fixed bugs:**

- Fix bad strategy of temporarily replacing async engines with synchronous engines in tests [\#57](https://github.com/ThibTrip/pangres/issues/57)

**Closed issues:**

- Changelog \(releases\) [\#58](https://github.com/ThibTrip/pangres/issues/58)

**Merged pull requests:**

- \[TESTS\] Tests overhaul [\#59](https://github.com/ThibTrip/pangres/pull/59) ([ThibTrip](https://github.com/ThibTrip))

## [v4.1](https://github.com/ThibTrip/pangres/releases/tag/v4.1) (2022-01-21)


**Release Notes**


___
**_New Features_**

* Added async support with function `pangres.aupsert` :rocket: ! Tested using `aiosqlite` for SQlite, `asyncpg` for PostgreSQL and `aiomysql` for MySQL. See documentation in dedicated wiki [page](https://github.com/ThibTrip/pangres/wiki/Aupsert)
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v4.0.2...v4.1)

**Implemented enhancements:**

- \[FEAT\] Add `pangres.aupsert` \(async variant\) [\#55](https://github.com/ThibTrip/pangres/pull/55) ([ThibTrip](https://github.com/ThibTrip))

**Closed issues:**

- Add async support? [\#47](https://github.com/ThibTrip/pangres/issues/47)

## [v4.0.2](https://github.com/ThibTrip/pangres/releases/tag/v4.0.2) (2022-01-17)


**Release Notes**


___
This patches an important bug with MySQL. We recommend that all users upgrade to this version.

**_Bug Fixes_**

* Fixed bug where tables in MySQL where created with auto increment on the primary key (see #56)
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v4.0.1...v4.0.2)

**Closed issues:**

- MySQL creating undesired auto incrementing primary keys [\#56](https://github.com/ThibTrip/pangres/issues/56)
- Get rid of the LooseVersion warning [\#54](https://github.com/ThibTrip/pangres/issues/54)

## [v4.0.1](https://github.com/ThibTrip/pangres/releases/tag/v4.0.1) (2022-01-13)


**Release Notes**


___
**_Bug Fixes_**

* removed warning due to deprecated code when checking versions of other libraries in Python >= 3.10 (see issue  #54)
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v4.0...v4.0.1)

## [v4.0](https://github.com/ThibTrip/pangres/releases/tag/v4.0) (2022-01-12)


**Release Notes**


___
There have been important changes in the main function **`pangres.upsert`**:

**_Breaking changes_**


1. The first argument **`engine`** has been be **renamed** to **`con`** and now accepts engines and connections
2. The argument **`chunksize`** now **defaults** to **`None`**. Like in `pandas.DataFrame.to_sql` we will attempt to insert all rows by default. Previously the default was `10000` rows.
3. There will be **no more automatic adjustments to the `chunksize`** the user passes in `pangres.upsert` even if we can predict that it will raise an Exception due to database limitations.

E.g. inserting 100000 rows at once in a SQlite database with `pangres` will necessarily raise an Exception down the line because we need to pass NUMBER_OF_ROWS * NUMBER_OF_COLUMNS parameters and the maximum of parameters allowed in SQLite is 32766 (for version >= 3.32.0, otherwise it is 999).

I have made a new utility function [**`pangres.adjust_chunksize`**](https://github.com/ThibTrip/pangres/wiki/Adjust-Chunksize) that you can use before calling **`pangres.upsert`** if you want to make sure the `chunksize` is not too big.

**_New Features_**


Now that `pangres.upsert` accept connections objects this will give you more control when using it when it comes to connections and transactions.

See [transaction control demo notebook](https://github.com/ThibTrip/pangres/blob/master/demos/transaction_control.ipynb).
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v3.1...v4.0)

**Closed issues:**

- Set default `chunksize` to None [\#51](https://github.com/ThibTrip/pangres/issues/51)
- Option to Disable commit in execute\(\) [\#44](https://github.com/ThibTrip/pangres/issues/44)

**Merged pull requests:**

- \[CORE\] Directly move to v4 [\#53](https://github.com/ThibTrip/pangres/pull/53) ([ThibTrip](https://github.com/ThibTrip))
- \[CORE\] Default `chunksize` to `None` [\#52](https://github.com/ThibTrip/pangres/pull/52) ([ThibTrip](https://github.com/ThibTrip))
- \[FEAT\] Add transaction and connection control [\#50](https://github.com/ThibTrip/pangres/pull/50) ([ThibTrip](https://github.com/ThibTrip))

## [v3.1](https://github.com/ThibTrip/pangres/releases/tag/v3.1) (2022-01-07)


**Release Notes**


___
**_Bug Fixes_**

* fixed wrong version number for SQLite when checking how many parameters are allowed (see commit [a60c61e](https://github.com/ThibTrip/pangres/commit/a60c61e205bb4c2b6509007595bd7bced9d91aa2))

**_Improvements_**

* when using `pangres.upsert` all operations will be done within a **transaction**
* when using `pangres.upsert` all operations will be done using a **single connection**
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v3.0...v3.1)

**Closed issues:**

- Regarding limit of Upserting [\#49](https://github.com/ThibTrip/pangres/issues/49)
- Using a transaction with only one connection [\#46](https://github.com/ThibTrip/pangres/issues/46)

**Merged pull requests:**

- \[FEAT\] Run all of pangres' operations at connection level and within a transaction [\#48](https://github.com/ThibTrip/pangres/pull/48) ([ThibTrip](https://github.com/ThibTrip))

## [v3.0](https://github.com/ThibTrip/pangres/releases/tag/v3.0) (2021-12-06)


**Release Notes**


___
**_New Features_**


* added option `create_table` to `pangres.upsert` for disabling table creation/check (`CREATE TABLE IF NOT EXISTS...` statement) that was always issued by this function. This can speed things up a little bit if you are sure that the target SQL table already exists
* `pangres` should already be compatible with the future `sqlalchemy` version 2 (unless something in the API changes in the future of course). You can try this by using the flag `future=True` in `sqlalchemy.create_engine`. For instance `create_engine('sqlite:///', future=True)`

**_Improvements_**


* allowed more SQL parameters for newer versions of SQLite (>=3.22.0): from 999 to 32766 (see issue #43)
* improved error messages (e.g. showing duplicated labels)

**_Bug Fixes_**


* when using parameter `yield_chunks=True` in `pangres.upsert` with an empty DataFrame we will now return an empty generator instead of None in order to ensure data type consistency
* fixed problem with log levels not being respected when pangres logs something (see commit #c494c95)
* fixed problem with the logger not filtering properly when the environment variable "PANGRES_LOG_LEVEL" is set (see commit #c494c95)

**_Breaking Changes_**


These changes are all related to new exceptions in `pangres`. If you weren't catching specific exceptions from `pangres` before this should not change anything for you.

* when `create_schema=True`, `pangres.upsert` will now raise the custom exception `pangres.exceptions.HasNoSchemaSystemException` if given database does not support schemas (AFAIK this only exist in PostgreSQL) but a schema was provided (`schema` is not None)
* in the presence of problematic column names (e.g. column names with "(", ")" or "%" for PostgreSQL) `pangres.upsert` will now raise the custom exception `pangres.exceptions.BadColumnNamesException`
* in the presence of duplicated labels (duplicates amongst columns/index levels or both) `pangres.upsert` and `pangres.fix_psycopg2_bad_cols` will now raise the custom exception `pangres.exceptions.DuplicateLabelsException`
* in the presence of unnamed index levels `pangres.upsert` and `pangres.fix_psycopg2_bad_cols` will now raise the custom exception `pangres.exceptions.UnnamedIndexLevelsException`
* in the presence of duplicates amongst the index values `pangres.upsert` will now raise the custom exception `pangres.exceptions.DuplicateValuesInIndexException`
* in `pangres.upsert`, when `add_missing_columns` is True but one of the columns to be added in the SQL table is part of the df's index, `pangres` will now raise the custom exception `pangres.exceptions.MissingIndexLevelInSqlException`

**_Documentation_**


* added notes on logging
* added a demo in the form of a notebook

**_Development_**


* rewrote the `upsert` module and separated the creation of the queries from their execution. The code should be easier to maintain and understand

**_Testing_**

* made it possible to test one or more databases types instead of always all of them
* added testing with `future=True` flag
* further improved coverage thanks to many new tests, ignoring coverage for tests (some functionalities of pytest caused coverage to be missed) and ignoring some lines
* added ids for tests to better read parameters used in pytest
* added context managers to ensure tables don't exist before and after tests in the database
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.3.1...v3.0)

**Closed issues:**

- SQLite3 Limit & Performance [\#43](https://github.com/ThibTrip/pangres/issues/43)
- Does not upsert [\#39](https://github.com/ThibTrip/pangres/issues/39)
- Upsert raises Index Error : [\#38](https://github.com/ThibTrip/pangres/issues/38)
- \[Question\] Is there any plan for testing upsert with sqlalchemy 2.0 API? [\#37](https://github.com/ThibTrip/pangres/issues/37)
- Update Wiki link to Pandas getting started guide [\#36](https://github.com/ThibTrip/pangres/issues/36)

**Merged pull requests:**

- \[FIX\] allow more SQL parameters for sqlite \>= 3.22.0 [\#45](https://github.com/ThibTrip/pangres/pull/45) ([ThibTrip](https://github.com/ThibTrip))
- \[FEAT\] Make the library compatible with sqlalchemy v2 [\#41](https://github.com/ThibTrip/pangres/pull/41) ([ThibTrip](https://github.com/ThibTrip))
- \[CORE\] New structure in preparation for Sqlalchemy v2.0 and asynchronous upsert [\#40](https://github.com/ThibTrip/pangres/pull/40) ([ThibTrip](https://github.com/ThibTrip))

## [v2.3.1](https://github.com/ThibTrip/pangres/releases/tag/v2.3.1) (2021-06-15)


**Release Notes**


___
**_Bugfixes_**

* Having a column named `values` will not raise errors anymore when using `pangres.upsert`. See issue #34
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.3...v2.3.1)

**Closed issues:**

- \[Bug\] db field name is keywords will raise `AttributeError: 'function' object has no attribute 'translate'` [\#34](https://github.com/ThibTrip/pangres/issues/34)

**Merged pull requests:**

- \[BUGFIX\] Make it possible to use a column named `values` [\#35](https://github.com/ThibTrip/pangres/pull/35) ([ThibTrip](https://github.com/ThibTrip))

## [v2.3](https://github.com/ThibTrip/pangres/releases/tag/v2.3) (2021-06-01)


**Release Notes**


___
**_New Features_**

* Added `yield_chunks` parameter to the main function `pangres.upsert`. When True, this will yield the result of inserted chunks (sqlalchemy.engine.cursor.LegacyCursorResult objects). This allows you to notably count upserted rows for each chunk. See issue #32.

**_Improvements_**

* The context manager for the connection to the database to make upserts was being called unnecessarily early. I have now put it at the very last step to minimize the connection time. See commit 4573588

**_Bugfixes_**

* The value of the `chunksize` parameter was mistakenly being modified when upserting to a SQlite database because of a wrong indentation in the code. See commit da2e9aa
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.2.4...v2.3)

**Closed issues:**

- Metric number of inserted rows [\#32](https://github.com/ThibTrip/pangres/issues/32)

**Merged pull requests:**

- \[FEAT\] Add yield chunks parameter \(for getting information on upserted chunks\) [\#33](https://github.com/ThibTrip/pangres/pull/33) ([ThibTrip](https://github.com/ThibTrip))

## [v2.2.4](https://github.com/ThibTrip/pangres/releases/tag/v2.2.4) (2021-05-01)


**Release Notes**


___
**_Bugfixes_**

* `pangres` is now **also** compatible with `sqlalchemy>=1.4`. **Important:** make sure to get `pandas>=1.2.4` as well to avoid deprecation warnings
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.2.3...v2.2.4)

**Closed issues:**

- Does the data frame index have to be the same as the primary key column? [\#29](https://github.com/ThibTrip/pangres/issues/29)
- Issue with "update" and "ignore" in Pangres [\#28](https://github.com/ThibTrip/pangres/issues/28)
- Error when trying to write a SQL database in MS Azure [\#25](https://github.com/ThibTrip/pangres/issues/25)

**Merged pull requests:**

- \[BUGFIX\] Handle sqalchemy v1.4 [\#31](https://github.com/ThibTrip/pangres/pull/31) ([ThibTrip](https://github.com/ThibTrip))

## [v2.2.3](https://github.com/ThibTrip/pangres/releases/tag/v2.2.3) (2020-12-15)


**Release Notes**


___
**_Bugfixes_**


* Fixed case where upsert was not possible with DataFrames that have no columns (only index) (see #26)
* Fixes failed release 2.2.2 (version commit was done afterwards)
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.2.2...v2.2.3)

**Closed issues:**

- how to write to a table with only one column [\#26](https://github.com/ThibTrip/pangres/issues/26)


[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.2.1...v2.2.2)

**Closed issues:**

- \[Question\] Write DataFrame index as a column argument [\#24](https://github.com/ThibTrip/pangres/issues/24)

**Merged pull requests:**

- \[BUGFIX\] Fix upsert with df with no columns \(only index\) [\#27](https://github.com/ThibTrip/pangres/pull/27) ([ThibTrip](https://github.com/ThibTrip))

## [v2.2.1](https://github.com/ThibTrip/pangres/releases/tag/v2.2.1) (2020-10-11)


**Release Notes**


___
**_Bugfixes_**

* fixed logging issue where pangres' logging formatting would take over once imported (it would override your own configuration, see #20)
* fixed logging issue where pangres' logs would not be written to file (see #18) 
* fixed conda environment file (the library <code>npdoc_to_md</code> has to be installed via <code>pip</code>, see commit [44d5423](https://github.com/ThibTrip/pangres/commit/44d54237981723cf1df909bc19495f8fdca83d08))

**_Improvements_**

* Added version attribute (see PR #22):

```python
import pangres
pangres.__version__
```
```
2.2.1
```
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.2...v2.2.1)

**Closed issues:**

- Pangres does something to the root logger that now everything identifies as pangres [\#20](https://github.com/ThibTrip/pangres/issues/20)
- Logging set up in utils.py can conflict with logging profiles in calling script [\#18](https://github.com/ThibTrip/pangres/issues/18)

**Merged pull requests:**

- \[ENH\]: add \_\_version\_\_ attribute [\#22](https://github.com/ThibTrip/pangres/pull/22) ([ThibTrip](https://github.com/ThibTrip))
- \[BUGFIX\]: Second attempt at fixing logging [\#21](https://github.com/ThibTrip/pangres/pull/21) ([ThibTrip](https://github.com/ThibTrip))
- \[BUGFIX\] Configure the logger instead of configuring logging [\#19](https://github.com/ThibTrip/pangres/pull/19) ([ThibTrip](https://github.com/ThibTrip))

## [v2.2](https://github.com/ThibTrip/pangres/releases/tag/v2.2) (2020-08-22)


**Release Notes**


___
**_Breaking changes_**

* Removal of previously deprecated function <code>pangres.pg_upsert</code> (use <code>pangres.upsert</code> instead)
* In function <code>pangres.upsert</code> the arguments <code>create_schema</code> and <code>add_new_columns</code> have been set to False by default (they were previously both set to True by default) as this would be the common expectation (i.e. failing on missing schema or missing column). See #15. Thanks to @lsloan and @rajrohan. 
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2.1...v2.2)

**Closed issues:**

- make `add_new_columns` default to `False` [\#15](https://github.com/ThibTrip/pangres/issues/15)
- It is actually compatible with unique constraint [\#12](https://github.com/ThibTrip/pangres/issues/12)
- Primary key requirement [\#11](https://github.com/ThibTrip/pangres/issues/11)

**Merged pull requests:**

- \[CORE\] Remove pg upsert as planned \(deprecated\) [\#17](https://github.com/ThibTrip/pangres/pull/17) ([ThibTrip](https://github.com/ThibTrip))
- \[CORE\] set create\_schema and add\_new\_columns to False by default [\#16](https://github.com/ThibTrip/pangres/pull/16) ([ThibTrip](https://github.com/ThibTrip))
- \[DOC\] Indicate that we can use unique keys and not just PKs [\#14](https://github.com/ThibTrip/pangres/pull/14) ([ThibTrip](https://github.com/ThibTrip))
- \[TEST\] Add tests that show pangres also works with unique keys [\#13](https://github.com/ThibTrip/pangres/pull/13) ([ThibTrip](https://github.com/ThibTrip))

## [v2.1](https://github.com/ThibTrip/pangres/releases/tag/v2.1) (2020-04-11)


**Release Notes**


___
**_Bugfixes_**

* Fixed bug where the "ON...CONFLICT" statement would be repeated leading to a syntax error in SQL insert statements

**_Changes for developers_**

* Testing instructions changed
* Added necessary tools to generate the documentation (see folder pangres/docs)
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v2...v2.1)

**Fixed bugs:**

- BUG: fix coverage [\#7](https://github.com/ThibTrip/pangres/issues/7)

**Merged pull requests:**

- TEST: Add testing via doctest [\#10](https://github.com/ThibTrip/pangres/pull/10) ([ThibTrip](https://github.com/ThibTrip))
- BUGFIX: Improve coverage [\#9](https://github.com/ThibTrip/pangres/pull/9) ([ThibTrip](https://github.com/ThibTrip))
- BUGFIX: fix issue where "ON CONFLICT" clause appears twice in the insert query [\#8](https://github.com/ThibTrip/pangres/pull/8) ([ThibTrip](https://github.com/ThibTrip))
- BUGFIX: fix bugs in tests [\#6](https://github.com/ThibTrip/pangres/pull/6) ([ThibTrip](https://github.com/ThibTrip))
- DOC: Fix broken anchor link [\#5](https://github.com/ThibTrip/pangres/pull/5) ([ThibTrip](https://github.com/ThibTrip))

## [v2](https://github.com/ThibTrip/pangres/releases/tag/v2) (2020-04-04)


**Release Notes**


___
**_New features_**


* Added support for MySQL and SQlite 🎉 !
* Completely SQL injection safe (everything is escaped or parameterized including schema, table and column names)
* Documentation improved
* Logo added ! (I am not much of a graphic designer but I did my best 🙈)

**_Deprecations_**

* pangres.pg_upsert became pangres.upsert (to reflect the fact that pangres can now handle other databases than postgres). **pangres.pg_upsert** will be removed in the next version!
* the argument "if_exists" of the old function pangres.pg_upsert was removed in the new pangres.upsert in favor of the argument "if_row_exists" whose functionnality is clearer. The equivalent of <code>if_exists="upsert_overwrite"</code> is now <code>if_row_exists="update"</code> and <code>if_exists="upsert_keep"</code> is now <code>if_row_exists="ignore"</code>

**_Breaking changes_**

* Contrary to the old function pangres.pg_upsert the new pangres.upsert function does not clean "bad" column names automatically for postgres. An error will be raised if any column contains "%" or "(" or ")" but you can use pangres.fix_psycopg2_bad_cols to fix such problems.

**_Changes for developers_**

* Testing improved! You can provide a connection string in the command line directly 👍 !
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v1.3.1...v2)

**Merged pull requests:**

- DOC: Update README.md [\#4](https://github.com/ThibTrip/pangres/pull/4) ([ThibTrip](https://github.com/ThibTrip))
- CORE: Add support for other databases \(MySQL, SQlite and potentially others\) [\#3](https://github.com/ThibTrip/pangres/pull/3) ([ThibTrip](https://github.com/ThibTrip))

## [v1.3.1](https://github.com/ThibTrip/pangres/releases/tag/v1.3.1) (2020-02-16)


**Release Notes**


___
* Drastically improved speed for small datasets (<= 100 rows and <= 20 columns)
___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/v1.3...v1.3.1)

**Merged pull requests:**

- CORE: Bump to version 1.3.1 [\#2](https://github.com/ThibTrip/pangres/pull/2) ([ThibTrip](https://github.com/ThibTrip))
- CORE:Improve performance [\#1](https://github.com/ThibTrip/pangres/pull/1) ([ThibTrip](https://github.com/ThibTrip))

## [v1.3](https://github.com/ThibTrip/pangres/releases/tag/v1.3) (2020-01-31)


**Release Notes**


___

___



[Full Changelog](https://github.com/ThibTrip/pangres/compare/6150b7b5374ba5aba25c42c6d253d18c0f7fc81f...v1.3)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*