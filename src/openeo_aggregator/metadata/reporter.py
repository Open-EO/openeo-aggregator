import inspect
import logging
import textwrap
from pathlib import Path
from typing import List, Tuple


def _extract_entity_id(**kwargs) -> Tuple[List[str], dict]:
    """
    Extract entity (collection/process) identifier (list of strings, like a path in a tree) from given kwargs.
    """
    entity_id = []
    for k in ["backend_id", "collection_id", "process_id"]:
        if k in kwargs:
            entity_id.append(kwargs.pop(k))
    return entity_id, kwargs


class Reporter:
    def report(self, message: str, **kwargs):
        """
        Formats the message then outputs it to some channel.
        Args:
            message: The message to report.
            **kwargs: Extra reporting parameters depending on reporter type.
        """
        raise NotImplementedError


class MarkDownReporter(Reporter):
    # TODO: add support for level/severity?

    def report(self, msg: str, **kwargs):
        """
        Formats the message as markdown then outputs it to the console.
        Args:
            msg: The message to report.
            **kwargs: Extra reporting parameters provided as key-value pairs.
                - diff: a list of strings representing a JSON diff
                - backend_id: the backend identifier
                - collection_id: the collection identifier
                - process_id: the process identifier
        """
        entity_id, kwargs = _extract_entity_id(**kwargs)
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
        print(msg)


class LoggerReporter(Reporter):
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def report(self, msg: str, **kwargs):
        """
        Outputs the message as a warning to the logger.

        Args:
            msg: The message to report.
            **kwargs: Extra reporting parameters provided as key-value pairs.
                - backend_id: the backend identifier
                - collection_id: the collection identifier
                - process_id: the process identifier
                - Any other parameters are printed as key-value pairs
        """
        entity_id, kwargs = _extract_entity_id(**kwargs)
        msg = f"[{':'.join(entity_id)}] {msg}"
        msg += f" ({kwargs})" if kwargs else ""
        # TODO: allow setting log level?
        self.logger.warning(msg)
