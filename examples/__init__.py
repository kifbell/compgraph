from .run_inverted_index import run_inverted_index
from .run_pmi import run_pmi
from .run_word_count import main
from .run_yandex_maps import run_yandex_maps

_ = main
_ = run_inverted_index
_ = run_yandex_maps
_ = run_pmi


__all__ = [
    "run_word_count",
    "run_inverted_index",
    "run_yandex_maps",
    "run_pmi",
]
