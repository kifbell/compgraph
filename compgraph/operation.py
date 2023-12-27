import abc
import typing as tp
from abc import ABC
from itertools import groupby

from compgraph.misc import TRow
from compgraph.misc import TRowsGenerator
from compgraph.misc import TRowsIterable


class Operation(ABC):
    @abc.abstractmethod
    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(
        self, filename: str, parser: tp.Callable[[str], TRowsGenerator]
    ) -> None:
        self._filename = filename
        self._parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self._filename) as f:
            for line in f:
                row = self._parser(line)
                # stupid mypy thinks that it's never gonna happen
                yield from row


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self._name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self._name]():
            yield row


# Operations


class Mapper(ABC):
    """Base class for mappers"""

    @abc.abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self._mapper = mapper

    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        for row in rows:
            yield from self._mapper(row)


class Reducer(ABC):
    """Base class for reducers"""

    @abc.abstractmethod
    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self._reducer = reducer
        self._keys = tuple(keys)

    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        for _, group in groupby(rows, key=lambda row: [row[key] for key in self._keys]):
            yield from self._reducer(self._keys, group)


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Mappers


# Reducers


# Joiners
