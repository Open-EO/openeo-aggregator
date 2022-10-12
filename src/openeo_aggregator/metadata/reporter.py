import inspect
from pathlib import Path


class ValidationReporter:
    def __init__(self):
        self.warning_messages = []
        self.critical_messages = []

    def report(self, msg, cid, backend="", level="warning"):
        caller = inspect.stack()[1]
        caller_file = Path(caller.filename).name.split("/")[-1]
        msg = f"[{cid}:{backend}] {caller_file}:{caller.lineno}: {msg}"
        self.critical_messages.append(msg) if level == "critical" else self.warning_messages.append(msg)

    def print(self):
        print("Warning messages:")
        for msg in self.warning_messages:
            print("  * {}".format(msg))
        print("Critical messages:")
        for msg in self.critical_messages:
            print("  * {}".format(msg))


class LoggerReporter:
    def __init__(self, logger):
        self.logger = logger

    def report(self, msg, cid, backend="", level="warning"):
        self.logger.warning(f"[{cid}:{backend}] {msg}")
