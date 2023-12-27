import enum
import typing as tp
from datetime import datetime

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class StringEnum(str, enum.Enum):
    """
    string enum
    """

    def __str__(self) -> str:
        return f"{self.value}"

    __repr__ = __str__


def get_valid_date(time: tp.Any, time_format: str) -> datetime:
    try:
        return datetime.strptime(time, time_format)
    except ValueError:
        return datetime.strptime(time, "%Y%m%dT%H%M%S")


class Columns(StringEnum):
    count = "count"  # type: ignore
    fraction = "fraction"
    frequency = "frequency"
    frequency_all = "frequency_all"
    log = "log"
    n_docs = "n_docs"
    presence_in_docs = "presence_in_docs"
