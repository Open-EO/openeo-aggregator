import inspect
import logging
import textwrap
from pathlib import Path
from typing import Tuple, List


def _extract_entity_id(**kwargs) -> Tuple[List[str], dict]:
    """
    Extract entity (collection/process) identifier (list of strings, like a path in a tree) from given kwargs.
    """
    entity_id = []
    for k in ["backend_id", "collection_id", "process_id"]:
        if k in kwargs:
            entity_id.append(kwargs.pop(k))
    return entity_id, kwargs


class MarkDownReporter:
    def __init__(self):
        # TODO Handle warning/critical level generically instead of with two lists?
        self.warning_messages = []
        self.critical_messages = []

    def report(self, msg: str, **kwargs):
        entity_id, kwargs = _extract_entity_id(**kwargs)
        level = kwargs.pop("level", "warning")
        caller = inspect.stack()[1]
        caller_file = Path(caller.filename).name.split("/")[-1]
        msg = f"- [ ] **{' : '.join(entity_id)}** ({caller_file}:{caller.lineno}): {msg}"
        diff = kwargs.pop("diff", None)
        if kwargs:
            msg += "\n" + ("\n".join(f"    - {k} `{v!r}`" for k, v in kwargs.items()))
        if diff:
            msg += "\n    - JSON diff:\n\n" + textwrap.indent("".join(diff), " " * 10)
        if "\n" in msg:
            # Multi-line message with sublist of code block: add extra newline
            msg += "\n"

        if level == "critical":
            self.critical_messages.append(msg)
        else:
            self.warning_messages.append(msg)

    def print(self):
        print(f"Critical messages ({len(self.critical_messages)}):")
        for msg in self.critical_messages:
            print(msg)
        print(f"Warning messages ({len(self.warning_messages)}):")
        for msg in self.warning_messages:
            print(msg)


class LoggerReporter:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def report(self, msg: str, **kwargs):
        entity_id, kwargs = _extract_entity_id(**kwargs)
        msg = f"[{':'.join(entity_id)}] {msg}"
        msg += f" ({kwargs})" if kwargs else ""
        # TODO: use report "level" to determine log level?
        self.logger.warning(msg)
