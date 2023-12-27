import abc
import typing as tp
from abc import ABC
from itertools import groupby

from compgraph.misc import TRow
from compgraph.misc import TRowsGenerator
from compgraph.misc import TRowsIterable
from compgraph.operation import Operation


def validate_suffix(
    key: str, suffix: str, another_row: TRow, duplicates: set[str] | None
) -> str:
    if duplicates is None:  # as it was before
        return key + suffix if key in another_row else key

    if key in duplicates or key in another_row:
        duplicates.add(key)
        return key + suffix
    return key


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = "_1", suffix_b: str = "_2") -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abc.abstractmethod
    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
        dups: set[str] | None = None,
    ) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _common_generator(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
        dups: set[str] | None,
    ) -> tp.Generator[TRow, None, bool]:
        a_is_empty = True
        for row_a in rows_a:
            a_is_empty = False
            for row_b in rows_b:
                result = {key: row_a[key] for key in keys}
                result.update(
                    {
                        validate_suffix(key, self._a_suffix, row_b, dups): value
                        for key, value in row_a.items()
                        if key not in keys
                    }
                )
                result.update(
                    {
                        validate_suffix(key, self._b_suffix, row_a, dups): value
                        for key, value in row_b.items()
                        if key not in keys
                    }
                )
                yield result
        return a_is_empty


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self._keys = tuple(keys)
        self._joiner = joiner

    def __call__(
        self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any
    ) -> TRowsGenerator:
        group_left = groupby(rows, key=lambda row: [row[key] for key in self._keys])
        group_right = groupby(args[0], key=lambda row: [row[key] for key in self._keys])

        left_key, left_row = next(group_left, (None, None))
        right_key, right_row = next(group_right, (None, None))

        left_key_prev = left_key
        right_key_prev = right_key

        duplicates: set[str] = set()

        while (
            left_row is not None
            and right_row is not None
            and left_key is not None  # dummy check for mypy
            and right_key is not None  # dummy check for mypy
            and left_key_prev is not None  # dummy check for mypy
            and right_key_prev is not None  # dummy check for mypy
        ):
            if left_key_prev > left_key:
                raise Exception("Left key not sorted")
            if right_key_prev > right_key:
                raise Exception("Right key not sorted")

            if left_key < right_key:
                yield from self._joiner(self._keys, left_row, iter([]), duplicates)
                left_key, left_row = next(group_left, (None, None))
            elif left_key == right_key:
                yield from self._joiner(self._keys, left_row, right_row, duplicates)
                left_key, left_row = next(group_left, (None, None))
                right_key, right_row = next(group_right, (None, None))
            else:
                yield from self._joiner(self._keys, iter([]), right_row, duplicates)
                right_key, right_row = next(group_right, (None, None))

            left_key_prev = left_key
            right_key_prev = right_key
        print(duplicates, duplicates)

        while left_row is not None and left_key is not None:  # use right suffix
            suffix = self._joiner._b_suffix
            for row in self._joiner(self._keys, left_row, iter([]), duplicates):
                yield {
                    key + suffix if key in duplicates else key: value
                    for key, value in row.items()
                }
            left_key, left_row = next(group_left, (None, None))

        while right_row is not None and right_key is not None:  # use left suffix
            suffix = self._joiner._a_suffix
            for row in self._joiner(self._keys, iter([]), right_row, duplicates):
                yield {
                    key + suffix if key in duplicates else key: value
                    for key, value in row.items()
                }
            right_key, right_row = next(group_right, (None, None))

        print(duplicates)
        return None


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
        dups: set[str] | None = None,
    ) -> TRowsGenerator:
        if unpacked_b := list(rows_b):
            yield from self._common_generator(
                keys, rows_a=rows_a, rows_b=unpacked_b, dups=dups
            )


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
        dups: set[str] | None = None,
    ) -> TRowsGenerator:
        rows_b = [row for row in rows_b]

        #
        # outer part
        #

        if not rows_b:
            yield from rows_a
        else:
            a_is_empty = yield from self._common_generator(
                keys=keys, rows_a=rows_a, rows_b=rows_b, dups=dups
            )
            if a_is_empty:
                if dups is None:
                    yield from rows_b
                    return
                for row in rows_b:
                    res: TRow = {}
                    for key, value in row.items():
                        print(f"{key =}, {value=}")
                        print(dups)
                        if key in dups:
                            key += self._b_suffix
                        res[key] = value
                    yield res


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
        dups: set[str] | None = None,
    ) -> TRowsGenerator:
        rows_b = [row for row in rows_b]

        #
        # left part
        #
        if not rows_b:
            yield from rows_a
        else:
            yield from self._common_generator(
                keys=keys, rows_a=rows_a, rows_b=rows_b, dups=dups
            )


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(
        self,
        keys: tp.Sequence[str],
        rows_a: TRowsIterable,
        rows_b: TRowsIterable,
        dups: set[str] | None = None,
    ) -> TRowsGenerator:
        rows_a = [row for row in rows_a]

        #
        # right  part
        #

        if not rows_a:
            yield from rows_b
        else:
            yield from self._common_generator(
                keys=keys, rows_a=rows_b, rows_b=rows_a, dups=dups
            )
