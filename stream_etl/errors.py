"""Error hierarchy for stream-etl.

Each operational error maps to a fixed exit code and a fixed stderr prefix
(``Error: <Type>``) so failure outcomes are deterministic and observable.
"""


class StreamETLError(Exception):
    """Base class for all stream-etl errors."""

    exit_code = 1
    type_name = "StreamETLError"

    def __init__(self, message=""):
        super().__init__(message)
        self.message = message

    def render(self):
        if self.message:
            return "Error: %s: %s" % (self.type_name, self.message)
        return "Error: %s" % self.type_name


class ConfigurationError(StreamETLError):
    """Invalid YAML configuration: missing keys, unknown ops, bad paths."""

    exit_code = 2
    type_name = "ConfigurationError"


class DataValidationError(StreamETLError):
    """A record is missing required keys, a path is absent, or cast failed."""

    exit_code = 3
    type_name = "DataValidationError"


class CheckpointError(StreamETLError):
    """Checkpoint is corrupt, from another config version, or unrecoverable."""

    exit_code = 4
    type_name = "CheckpointError"


class SourceError(StreamETLError):
    """An input source cannot be read."""

    exit_code = 5
    type_name = "SourceError"


class SinkError(StreamETLError):
    """The output cannot be written."""

    exit_code = 5
    type_name = "SinkError"
