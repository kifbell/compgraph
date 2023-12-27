# Mappers
import calendar
import re
import string
import typing as tp

import math

from compgraph.misc import get_valid_date
from compgraph.misc import TRow
from compgraph.misc import TRowsGenerator
from compgraph.operation import Mapper


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self._column] = "".join(
            char for char in row[self._column] if char not in string.punctuation
        )
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self._column] = row[self._column].lower()
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self._column = column
        self._separator = "\\s+" if separator is None else separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self._separator == "\\s+":
            s = row[self._column] + " "
        else:
            s = row[self._column] + self._separator

        start = 0
        for match in re.finditer(self._separator, s):
            row_copy = row.copy()
            row_copy[self._column] = s[start: match.start()]
            yield row_copy
            start = match.end()


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(
        self, columns: tp.Sequence[str], result_column: str = "product"
    ) -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self._columns = columns
        self._result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result = 1
        for column in self._columns:
            result *= row[column]
        row[self._result_column] = result
        yield row


class NaturalLog(Mapper):
    """Calculates NaturalLog of column"""

    def __init__(self, column: str, result_column: str = "product") -> None:
        """
        :param column: column names to get natural log
        :param result_column: column name to save result in
        """
        self._column = column
        self._result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self._result_column] = math.log(row[self._column])
        yield row


class Divide(Mapper):
    """Calculates division one column by another"""

    def __init__(
        self, nominator: str, denominator: str, result_column: str = "product"
    ) -> None:
        """
        :param nominator: column name to be nominator
        :param denominator: column name to be denominator
        :param result_column: column name to save division in
        """
        self._nominator = nominator
        self._denominator = denominator
        self._result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self._result_column] = row[self._nominator] / row[self._denominator]
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self._condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self._condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self._columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        result: TRow = {}
        for column in self._columns:
            result[column] = row[column]
        yield result


class ParseTime(Mapper):
    """Parse column and save weekday and hour in results columns"""

    WEEKDAYS = list(calendar.day_abbr)

    def __init__(
        self,
        time_column: str,
        time_format: str,
        weekday_result_column: str,
        hour_result_column: str,
    ) -> None:
        """
        :param time_column: time to parse
        :param time_format:
        :param weekday_result_column: result columns for weekday
        :param hour_result_column: result columns for hour
        """
        self._time_column = time_column
        self._time_format = time_format
        self._weekday_result = weekday_result_column
        self._hour_result = hour_result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        dt = get_valid_date(row[self._time_column], self._time_format)
        row[self._weekday_result] = self.WEEKDAYS[dt.weekday()]
        row[self._hour_result] = dt.hour
        yield row


class CalcHaversine(Mapper):
    """Parse column and save weekday and hour in results columns"""

    EARTH_RADIUS_KM = 6371.0

    def __init__(self, start: str, end: str, result: str) -> None:
        """
        :param start: column name of [lon, lat] of start point
        :param end: column name of [lon, lat] of end point
        :param result: result column name
        """
        self._start = start
        self._end = end
        self._result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        lon1, lat1 = row[self._start]
        lon2, lat2 = row[self._end]
        row[self._result] = self.haversine(lon1, lat1, lon2, lat2)
        yield row

    @staticmethod
    def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """

        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))
        return c * CalcHaversine.EARTH_RADIUS_KM
