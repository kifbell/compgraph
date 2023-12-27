import heapq
import typing as tp

from compgraph.misc import get_valid_date
from compgraph.misc import TRowsGenerator
from compgraph.misc import TRowsIterable
from compgraph.operation import Reducer


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self._column_max = column
        self.n = n

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        heap: tp.List[tp.Tuple[tp.Any, tp.List[tp.Tuple[str, tp.Any]]]] = []
        for row in rows:
            row_values = [
                (key, value) for key, value in row.items() if key != self._column_max
            ]
            heapq.heappush(heap, (row[self._column_max], row_values))
            if len(heap) > self.n:
                heapq.heappop(heap)

        heap.reverse()
        while len(heap) > 0:
            row_values = heap[0][1]
            max_value = heap[0][0]
            yield {key: value for key, value in row_values} | {
                self._column_max: max_value
            }
            heapq.heappop(heap)


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = "tf") -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self._words_column = words_column
        self._result_column = result_column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        map_key_values: dict[str, tp.Any] = {}
        counter: dict[str, int] = {}
        n = 0
        for row in rows:
            n += 1
            if len(map_key_values) == 0:
                map_key_values = {
                    key: value for key, value in row.items() if key in group_key
                }
            counter[row[self._words_column]] = (
                counter.get(row[self._words_column], 0) + 1
            )

        #
        # result format
        #

        result = [
            {self._words_column: word, self._result_column: count / n} | map_key_values
            for word, count in counter.items()
        ]

        yield from result


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self._column = column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        map_key_values: dict[str, tp.Any] = {}
        n = 0
        for row in rows:
            n += 1
            if len(map_key_values) == 0:
                map_key_values = {
                    key: value for key, value in row.items() if key in group_key
                }
        yield {"count": n} | map_key_values


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self._column = column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        n = 0
        map_key_values: dict[str, tp.Any] = {}
        for row in rows:
            if not map_key_values:
                map_key_values = {
                    key: value for key, value in row.items() if key in group_key
                }
            n += row[self._column]
        yield {self._column: n} | map_key_values


class NUnique(Reducer):
    """
    Count number of unique elements in specified column
    """

    def __init__(self, column: str, result_column: str) -> None:
        """
        :param column: name for result column
        :param result_column: name for result column
        """
        self._column = column
        self._result_column = result_column

    def __call__(
        self, group_key: tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        unique_value: tp.Set[tp.Any] = set()
        map_key_values: tp.Dict[str, tp.Any] = {}
        for row in rows:
            if not map_key_values:
                map_key_values = {
                    key: value for key, value in row.items() if key in group_key
                }
            unique_value.add(row[self._column])
        yield {self._result_column: len(unique_value)} | map_key_values


class Speed(Reducer):
    """
    Calculate avarage speed for specific weekday and hour
    """

    SECONDS_IN_HOUR = 3600

    def __init__(
        self,
        length: str,
        enter_time: str,
        leave_time: str,
        time_format: str,
        result_column: str,
    ) -> None:
        """
        :param length:  length of the road
        :param enter_time: time of entering the road
        :param leave_time: time of leaving the road
        :param time_format: time format
        :param result_column: result column name
        """
        self.length = length
        self.enter_time = enter_time
        self.leave_time = leave_time
        self._time_format = time_format
        self._result_column = result_column

    def __call__(
        self, group_key: tp.Tuple[str, ...], rows: TRowsIterable
    ) -> TRowsGenerator:
        total_length: float = 0
        total_time: float = 0

        map_key_values: tp.Dict[str, tp.Any] = {}
        for row in rows:
            if not map_key_values:
                map_key_values = {
                    key: value for key, value in row.items() if key in group_key
                }

            dt_1 = get_valid_date(row[self.enter_time], self._time_format)
            dt_2 = get_valid_date(row[self.leave_time], self._time_format)
            time_delta = dt_2 - dt_1

            total_time += (
                time_delta.seconds + time_delta.microseconds * 10 ** (-6)
            ) / self.SECONDS_IN_HOUR
            total_length += row[self.length]

        yield map_key_values | {self._result_column: total_length / total_time}
