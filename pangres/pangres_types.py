"""
Types and type aliases for pangres
"""
from typing import AsyncIterable, Iterable, TYPE_CHECKING, TypeVar, Union, Any
from sqlalchemy.engine import Connection, Engine
# local imports
from pangres.helpers import _sqla_gt20


if TYPE_CHECKING and _sqla_gt20():
    #  ignore F401 because some types imported here are only used in other modules
    from sqlalchemy.engine.cursor import CursorResult
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncTransaction  # noqa: F401
    from sqlalchemy.ext.asyncio.engine import AsyncConnectable  # noqa: F401
    UpsertResult = Union[None, Iterable[CursorResult]]
    AUpsertResult = Union[None, AsyncIterable[CursorResult]]
    AsyncConnectionOrAsyncEngine = Union[AsyncConnection, AsyncEngine]
else:
    # type ignore because repeated assignment is OK in this case
    AsyncConnection = TypeVar('AsyncConnection', bound=Any)  # type: ignore
    AsyncEngine = TypeVar('AsyncEngine', bound=Any)  # type: ignore
    CursorResult = TypeVar('CursorResult', bound=Any)  # type: ignore
    AsyncTransaction = TypeVar('AsyncTransaction', bound=Any)  # type: ignore
    AsyncConnectable = Union[AsyncEngine, AsyncConnection]  # type: ignore
    UpsertResult = Union[None, Iterable[CursorResult]]  # type: ignore
    AUpsertResult = Union[None, AsyncIterable[CursorResult]]  # type: ignore
    AsyncConnectionOrAsyncEngine = Union[AsyncConnection, AsyncEngine]  # type: ignore

ConnectionOrEngine = Union[Connection, Engine]
