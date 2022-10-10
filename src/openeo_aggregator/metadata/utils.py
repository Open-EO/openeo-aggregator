import logging
import inspect
from typing import Any, Dict, Tuple, List

class Unset:
    def __bool__(self) -> bool:
        return False
UNSET: Unset = Unset()


def concat(
        dictionaries: List[Tuple[str, Dict]],
        key, expected_types=None, report=logging.getLogger().warning
):
    """
    Concatenate all values of a given key from a list of dictionaries, skipping any duplicates.
    Args:
        dictionaries: List of dictionaries, given as tuples of (collection_identifier, dictionary)
        key: Key to concatenate
        expected_types: List of expected types for the value of the key, if None, all types are allowed
            When a value is not of the expected type, it is reported and ignored.
        report: function to report inconsistencies

    Returns:
        Concatenated value
    """
    result = None
    for cid, d in dictionaries:
        if key in d:
            if expected_types is not None:
                for expected_type in expected_types:
                    if not isinstance(d[key], expected_type):
                        caller = inspect.stack()[1]
                        report(f"[{cid}]: Unexpected type for {key!r}: {type(d[key])!r} instead of {expected_type!r} in {caller.filename}:{caller.lineno}")
            if result is None:
                result = d[key]
            else:
                if isinstance(result, list):
                    if d[key] in result:
                        continue
                    result = list(result + d[key])
                elif isinstance(result, dict):
                    result = {**result, **d[key]}
                elif hasattr(result, 'merge'):
                    result = result.merge(d[key])
                else:
                    caller = inspect.stack()[1]
                    report(f"[{cid}]: Unhandled merging of {key!r} with {type(result)} and {type(d[key])} in {caller.filename}:{caller.lineno}")
    return result
