import logging
import inspect
from typing import Any, Dict, Tuple, List, Callable


class Unset:
    def __bool__(self) -> bool:
        return False
UNSET: Unset = Unset()


def merge_lists_skip_duplicates(lists: List[List[Any]]) -> List[Any]:
    """
    Merge multiple lists into one, but only keep unique values.
    :param lists: list of lists to merge
    :return: merged list
    """
    merged = []
    for l in lists:
        for v in l:
            if v not in merged:
                merged.append(v)
    return merged
