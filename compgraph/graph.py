import typing as tp
from copy import copy

from compgraph.joiner import Join
from compgraph.joiner import Joiner
from compgraph.operation import Map
from compgraph.operation import Mapper
from compgraph.operation import Reduce
from compgraph.operation import Reducer
from .external_sort import ExternalSort
from .misc import TRowsGenerator
from .operation import Operation
from .operation import Read
from .operation import ReadIterFactory
from .operation import TRowsIterable


def call_single_method(
    func: Operation,
    result: TRowsIterable,
    join_params_temp: list["Graph"],
    **kwargs: tp.Any,
) -> TRowsGenerator:
    if isinstance(func, Join):
        return func(result, join_params_temp.pop().run(**kwargs))
        # print("status")
    return func(result)


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self._operations: list[Operation] = list()
        self._join_params: list["Graph"] = list()

    def update_ops(
        self, *operations: Operation, join_params: tp.Optional["Graph"] = None
    ) -> "Graph":
        self._operations = list(operations) + self._operations
        if join_params:
            self._join_params = [join_params] + self._join_params
        return self

    @staticmethod
    def graph_from_iter(name: str) -> "Graph":
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        return Graph().update_ops(copy(ReadIterFactory(name)))

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], TRowsGenerator]) -> "Graph":
        """Construct new graph extended with operation for reading rows from file
        Use Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        return Graph().update_ops(copy(Read(filename, parser)))

    def map(self, mapper: Mapper) -> "Graph":
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        if not self._operations:
            raise ValueError("graph has no data source")

        return self.update_ops(copy(Map(mapper)))

    def reduce(self, reducer: Reducer, keys: tp.Sequence[str]) -> "Graph":
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        if not self._operations:
            raise ValueError("graph has no data source")

        return self.update_ops(copy(Reduce(reducer, keys)))

    def sort(self, keys: tp.Sequence[str]) -> "Graph":
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        if not self._operations:
            raise ValueError("graph has no data source")

        return self.update_ops(copy(ExternalSort(keys)))

    def join(
        self, joiner: Joiner, join_graph: "Graph", keys: tp.Sequence[str]
    ) -> "Graph":
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        if not self._operations:
            raise ValueError("graph has no data source")
        return self.update_ops(Join(joiner, keys), join_params=join_graph)

    def run(self, **kwargs: tp.Any) -> TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        if not self._operations:
            raise ValueError("graph has no data source")

        join_params_temp = self._join_params.copy()
        result: TRowsGenerator = self._operations[-1](**kwargs)
        for func in self._operations[-2::-1]:
            result = call_single_method(func, result, join_params_temp, **kwargs)

        yield from result
