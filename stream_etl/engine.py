"""The ETL engine: run, replay, transforms, schema evolution, checkpoints.

Two source types are supported:

* ``jsonl``: each input line is a JSON object carrying ``source_id`` /
  ``event_id`` / ``payload``;
* ``csv``: UTF-8 CSV (optional BOM) whose first logical record is a unique,
  non-empty header containing ``source_id`` and ``event_id``; the remaining
  columns form a flat string payload and the two control columns stay out of
  ``data``.

For every input record the configured transforms are applied to the payload
in order (shared transforms first, then the source's own), and one output
object is emitted::

    {"source_id": ..., "event_id": ..., "schema_version": N, "data": {...}}

A ``filter`` transform instead decides whether the record survives: when its
condition fails the input record is still consumed (it counts toward the
batch and the committed prefix) but emits no output and no schema change.

A source may set ``dedup_window`` to a positive integer N. Then, after
envelope validation and before any transform, the record's ``event_id`` is
looked up among the source's previous N-1 consumed records using JSON scalar
equality. A duplicate is consumed exactly like a filtered record (it counts
toward the batch and the committed prefix but emits no output and no schema
change); the first occurrence of an id enters the window even if a filter
later drops it, so later matching records are still duplicates. N of 1 keeps
an empty window and never de-duplicates. Windows are per source and never
interact across sources.

Sources are processed in configuration order. Every ``batch_size`` records
(and at each source boundary) the output is fsynced and the checkpoint is
atomically replaced. The checkpoint stores the byte offset of the last
committed record boundary together with a hash of the input prefix up to it
and, for de-duplicating sources, the complete sliding window of recent
event ids; replay verifies that prefix is unchanged and the file has not
shrunk, truncates any uncommitted tail of the output using the sink byte
offset, then continues, so records are never duplicated or skipped.
"""

import hashlib
import json
import os
import tempfile
from collections import deque

from .config import is_finite_number, split_path
from .errors import (
    CheckpointError,
    DataValidationError,
    SinkError,
    SourceError,
)

CHECKPOINT_VERSION = 2
ENCODING = "utf-8"
BOM = b"\xef\xbb\xbf"
EMPTY_PREFIX_HASH = hashlib.sha256(b"").hexdigest()


# --------------------------------------------------------------------------
# Field path helpers
# --------------------------------------------------------------------------


def _desc(parts):
    return ".".join(parts)


def _walk(data, parts, where):
    """Walk a path that must exist through nested dicts."""
    cur = data
    for part in parts:
        if not isinstance(cur, dict) or part not in cur:
            raise DataValidationError("%s: path %r does not exist"
                                      % (where, _desc(parts)))
        cur = cur[part]
    return cur


def _parent_existing(data, parts, where):
    """Parent container must exist; leaf key returned (leaf may be absent)."""
    parent = _walk(data, parts[:-1], where)
    if not isinstance(parent, dict):
        raise DataValidationError("%s: path %r does not exist"
                                  % (where, _desc(parts)))
    return parent, parts[-1]


def _parent_and_key(data, parts, where):
    """Parent container and leaf must both exist (drop/cast/rename-from)."""
    parent, key = _parent_existing(data, parts, where)
    if key not in parent:
        raise DataValidationError("%s: path %r does not exist"
                                  % (where, _desc(parts)))
    return parent, key


# --------------------------------------------------------------------------
# Casts
# --------------------------------------------------------------------------


def _cast_integer(value, where):
    if isinstance(value, bool) or value is None:
        raise DataValidationError("%s: cannot cast %r to integer" % (where, value))
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise DataValidationError("%s: cannot cast %r to integer" % (where, value))
    if isinstance(value, str):
        text = value.strip()
        body = text[1:] if text[:1] in ("+", "-") else text
        if body.isdigit():
            return int(text)
    raise DataValidationError("%s: cannot cast %r to integer" % (where, value))


def _finite_float(text, where, value):
    try:
        result = float(text)
    except ValueError:
        result = None
    if result is None or result != result or result in (
        float("inf"),
        float("-inf"),
    ):
        raise DataValidationError("%s: cannot cast %r to number" % (where, value))
    return result


def _cast_number(value, where):
    if isinstance(value, bool) or value is None:
        raise DataValidationError("%s: cannot cast %r to number" % (where, value))
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        return _finite_float(value, where, value)
    if isinstance(value, str):
        return _finite_float(value.strip(), where, value)
    raise DataValidationError("%s: cannot cast %r to number" % (where, value))


def _cast_boolean(value, where):
    if isinstance(value, bool):
        return value
    if value is None:
        raise DataValidationError("%s: cannot cast null to boolean" % where)
    if isinstance(value, str):
        low = value.strip()
        if low == "true":
            return True
        if low == "false":
            return False
    elif isinstance(value, int) and value in (0, 1):
        return bool(value)
    elif isinstance(value, float) and value in (0.0, 1.0):
        return bool(value)
    raise DataValidationError("%s: cannot cast %r to boolean" % (where, value))


def _cast_string(value, where):
    if value is None or isinstance(value, (dict, list)):
        raise DataValidationError("%s: cannot cast %r to string" % (where, value))
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return json.dumps(value)


def _cast_value(value, target, where):
    if target == "string":
        return _cast_string(value, where)
    if target == "integer":
        return _cast_integer(value, where)
    if target == "number":
        return _cast_number(value, where)
    return _cast_boolean(value, where)


# --------------------------------------------------------------------------
# Filters
# --------------------------------------------------------------------------


def _scalar_equal(left, right):
    """JSON scalar equality.

    ``null`` equals only ``null``; strings compare by exact content; booleans
    equal only booleans; ints and floats compare by numeric value and a
    boolean is never a number.
    """
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) \
            and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return isinstance(left, str) and isinstance(right, str) and left == right


def _filter_matches(data, raw, where):
    """Evaluate one ``filter`` transform; True keeps the record."""
    parts = split_path(raw["field"])
    value = _walk(data, parts, where)
    if isinstance(value, (dict, list)):
        raise DataValidationError(
            "%s: field %r is not a scalar" % (where, _desc(parts)))
    if isinstance(value, float) and not is_finite_number(value):
        raise DataValidationError(
            "%s: field %r is not a finite number" % (where, _desc(parts)))
    compare = raw["compare"]
    target = raw["value"]
    if compare == "eq":
        return _scalar_equal(value, target)
    if compare == "ne":
        return not _scalar_equal(value, target)
    # Ordering compares accept only finite non-boolean numbers on both
    # sides; the configured value was validated at load time.
    if not is_finite_number(value):
        raise DataValidationError(
            "%s: compare %r requires a finite numeric field, got %r"
            % (where, compare, value))
    if compare == "lt":
        return value < target
    if compare == "lte":
        return value <= target
    if compare == "gt":
        return value > target
    return value >= target


# --------------------------------------------------------------------------
# Transforms
# --------------------------------------------------------------------------


def apply_transforms(data, transforms, where):
    """Apply transforms in order to ``data`` (mutated in place).

    Returns the transformed data, or ``None`` when a ``filter`` transform
    drops the record (the input record then produces no output).
    """
    for raw in transforms:
        op = raw["op"]
        if op == "rename":
            parts_from = split_path(raw["from"])
            parts_to = split_path(raw["to"])
            value = _walk(data, parts_from, where)
            dest_parent, dest_key = _parent_existing(data, parts_to, where)
            if dest_key in dest_parent:
                # renaming over an existing field would silently lose data
                raise DataValidationError(
                    "%s: rename destination %r already exists"
                    % (where, _desc(parts_to))
                )
            src_parent, src_key = _parent_and_key(data, parts_from, where)
            del src_parent[src_key]
            dest_parent[dest_key] = value
        elif op == "drop":
            parts = split_path(raw["field"])
            parent, key = _parent_and_key(data, parts, where)
            del parent[key]
        elif op == "set":
            parts = split_path(raw["field"])
            parent, key = _parent_existing(data, parts, where)
            parent[key] = raw["value"]
        elif op == "filter":
            if not _filter_matches(data, raw, where):
                return None
        else:  # cast
            parts = split_path(raw["field"])
            parent, key = _parent_and_key(data, parts, where)
            parent[key] = _cast_value(parent[key], raw["type"], where + " (cast)")
    return data


# --------------------------------------------------------------------------
# Structural schema fingerprint
# --------------------------------------------------------------------------


def _schema_shape(value):
    """Type/structure descriptor independent of concrete leaf values.

    Dicts contribute their key set and each value's shape; arrays contribute
    the distinct shapes of their elements (order- and multiplicity-free, so
    growing an array does not look like a schema change, but a new kind of
    nested object inside one does).
    """
    if isinstance(value, dict):
        return {"dict": {k: _schema_shape(value[k]) for k in value}}
    if isinstance(value, list):
        shapes = {
            json.dumps(_schema_shape(v), sort_keys=True, separators=(",", ":"))
            for v in value
        }
        return {"list": sorted(shapes)}
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def schema_fingerprint(value):
    return json.dumps(_schema_shape(value), ensure_ascii=False,
                      sort_keys=True, separators=(",", ":"))


# --------------------------------------------------------------------------
# Per-source state and checkpoint (de)serialisation
# --------------------------------------------------------------------------


class SourceState:
    def __init__(self, spec):
        self.spec = spec
        # None means de-duplication is disabled; otherwise N (positive int)
        # and a queue of the event ids of the last N-1 consumed records,
        # oldest first.
        self.dedup_window = spec.get("dedup_window")
        self.dedup_keys = deque()
        self.offset = 0
        self.input_hash = EMPTY_PREFIX_HASH
        self.records = 0
        self.schema_version = 0
        self.schema_fingerprint = None

    def note_schema(self, fp):
        if fp != self.schema_fingerprint:
            self.schema_version += 1
            self.schema_fingerprint = fp

    # -- bounded de-duplication ------------------------------------------

    def is_duplicate(self, event_id):
        """True when ``event_id`` matches one of the last N-1 records.

        Disabled sources (no ``dedup_window``) and N of 1 (an empty history)
        never match. Comparison uses the same JSON scalar equality as
        filters.
        """
        if self.dedup_window is None:
            return False
        for past in self.dedup_keys:
            if _scalar_equal(event_id, past):
                return True
        return False

    def observe(self, event_id):
        """Slide the window over one just-consumed input record.

        Every consumed input record -- first occurrence, duplicate or
        filtered-out -- shifts the window, so the window always holds the
        last N-1 *consumed* records.
        """
        if self.dedup_window is None:
            return
        self.dedup_keys.append(event_id)
        if len(self.dedup_keys) >= self.dedup_window:
            self.dedup_keys.popleft()

    def to_json(self):
        return {
            "path": self.spec["path"],
            "offset": self.offset,
            "input_hash": self.input_hash,
            "records": self.records,
            "schema_version": self.schema_version,
            "schema_fingerprint": self.schema_fingerprint,
            "dedup_window": self.dedup_window,
            "dedup_keys": list(self.dedup_keys),
        }

    @classmethod
    def from_json(cls, spec, raw):
        state = cls(spec)
        if not isinstance(raw, dict):
            raise CheckpointError("checkpoint entry for source %r is invalid"
                                  % spec["id"])
        required = ("path", "offset", "input_hash", "records",
                    "schema_version", "schema_fingerprint")
        for key in required:
            if key not in raw:
                raise CheckpointError(
                    "checkpoint entry for source %r is missing %r"
                    % (spec["id"], key)
                )
        if raw["path"] != spec["path"]:
            raise CheckpointError(
                "source %r path differs from the one in the checkpoint"
                % spec["id"]
            )
        for key in ("offset", "records", "schema_version"):
            val = raw[key]
            if isinstance(val, bool) or not isinstance(val, int) or val < 0:
                raise CheckpointError(
                    "checkpoint %s for source %r is invalid"
                    % (key, spec["id"])
                )
        prefix_hash = raw["input_hash"]
        if not isinstance(prefix_hash, str) or len(prefix_hash) != 64:
            raise CheckpointError(
                "checkpoint input_hash for source %r is invalid"
                % spec["id"]
            )
        try:
            bytes.fromhex(prefix_hash)
        except ValueError:
            raise CheckpointError(
                "checkpoint input_hash for source %r is invalid"
                % spec["id"]
            )
        fp = raw["schema_fingerprint"]
        if fp is not None and not isinstance(fp, str):
            raise CheckpointError(
                "checkpoint schema_fingerprint for source %r is invalid"
                % spec["id"]
            )
        state.offset = raw["offset"]
        state.input_hash = prefix_hash
        state.records = raw["records"]
        state.schema_version = raw["schema_version"]
        state.schema_fingerprint = fp
        state._load_dedup_state(spec, raw)
        return state

    def _load_dedup_state(self, spec, raw):
        """Validate and restore the sliding window from checkpoint data."""
        configured = spec.get("dedup_window")
        has_window_field = "dedup_window" in raw
        has_keys_field = "dedup_keys" in raw
        saved_window = raw.get("dedup_window")
        saved_keys = raw.get("dedup_keys")

        if configured is None:
            # Checkpoints written before de-duplication existed carry
            # neither field; checkpoints written by this code carry an
            # explicit null/empty window. Anything else means the window
            # state does not match the configuration.
            if has_window_field and saved_window is not None:
                raise CheckpointError(
                    "checkpoint dedup_window for source %r does not match "
                    "the configuration" % spec["id"]
                )
            if has_keys_field and saved_keys != []:
                raise CheckpointError(
                    "checkpoint dedup state for source %r does not match "
                    "the configuration" % spec["id"]
                )
            return

        if not has_window_field or not has_keys_field:
            raise CheckpointError(
                "checkpoint entry for source %r is missing dedup window "
                "state" % spec["id"]
            )
        if (isinstance(saved_window, bool)
                or not isinstance(saved_window, int)
                or saved_window != configured):
            raise CheckpointError(
                "checkpoint dedup_window for source %r does not match the "
                "configuration" % spec["id"]
            )
        if not isinstance(saved_keys, list):
            raise CheckpointError(
                "checkpoint dedup_keys for source %r is invalid"
                % spec["id"]
            )
        for value in saved_keys:
            # Event ids are JSON scalars (the envelope rejects only
            # arrays/objects; null is a legal key and matches only null).
            if value is not None and not isinstance(
                    value, (bool, int, float, str)):
                raise CheckpointError(
                    "checkpoint dedup_keys for source %r contains an "
                    "invalid event_id" % spec["id"]
                )
        # The window is exactly the suffix of the consumed record stream;
        # its size is therefore fully determined by N and the record count.
        expected = min(configured - 1, self.records)
        if len(saved_keys) != expected:
            raise CheckpointError(
                "checkpoint dedup window for source %r is inconsistent "
                "with its record count" % spec["id"]
            )
        self.dedup_window = configured
        self.dedup_keys = deque(saved_keys)


class Checkpoint:
    def __init__(self, config, states, sink_offset):
        self.config = config
        self.states = states
        self.sink_offset = sink_offset

    def to_json(self):
        return {
            "version": CHECKPOINT_VERSION,
            "config_fingerprint": self.config.fingerprint(),
            "sink_offset": self.sink_offset,
            "sources": {
                spec["id"]: state.to_json()
                for spec, state in zip(self.config.sources, self.states)
            },
        }

    def write_atomic(self, path):
        directory = os.path.dirname(os.path.abspath(path)) or "."
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(
                prefix=".stream-etl-cp-", suffix=".tmp", dir=directory
            )
            with os.fdopen(fd, "wb") as fp:
                fp.write((json.dumps(self.to_json(), ensure_ascii=False)
                          + "\n").encode(ENCODING))
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp, path)
            tmp = None
            try:
                dir_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        except OSError as exc:
            raise SinkError("cannot write checkpoint %s: %s" % (path, exc))
        finally:
            if tmp is not None:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass


def load_checkpoint(path, config):
    try:
        with open(path, "rb") as fp:
            raw = fp.read()
    except OSError as exc:
        raise CheckpointError("cannot read checkpoint %s: %s" % (path, exc))
    try:
        doc = json.loads(raw.decode(ENCODING))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CheckpointError("checkpoint %s is corrupt" % path)
    if not isinstance(doc, dict):
        raise CheckpointError("checkpoint %s is corrupt" % path)
    if doc.get("version") != CHECKPOINT_VERSION:
        raise CheckpointError("checkpoint version mismatch (expected %d, got %r)"
                              % (CHECKPOINT_VERSION, doc.get("version")))
    if doc.get("config_fingerprint") != config.fingerprint():
        raise CheckpointError("checkpoint was written by a different configuration")
    sink_offset = doc.get("sink_offset")
    if (isinstance(sink_offset, bool) or not isinstance(sink_offset, int)
            or sink_offset < 0):
        raise CheckpointError("checkpoint sink_offset is invalid")
    raw_sources = doc.get("sources")
    if not isinstance(raw_sources, dict):
        raise CheckpointError("checkpoint sources section is invalid")
    by_id = {spec["id"]: spec for spec in config.sources}
    if set(raw_sources) != set(by_id):
        raise CheckpointError("checkpoint sources do not match configuration")
    states = [
        SourceState.from_json(by_id[spec["id"]], raw_sources[spec["id"]])
        for spec in config.sources
    ]
    return Checkpoint(config, states, sink_offset)


# --------------------------------------------------------------------------
# Filesystem preflight
# --------------------------------------------------------------------------


def _guard_paths(config, output_path, checkpoint_path):
    out_abs = os.path.abspath(output_path)
    cp_abs = os.path.abspath(checkpoint_path)
    if out_abs == cp_abs:
        from .errors import ConfigurationError
        raise ConfigurationError("output and checkpoint paths must differ")
    input_abs = {os.path.abspath(spec["path"]) for spec in config.sources}
    if out_abs in input_abs:
        from .errors import ConfigurationError
        raise ConfigurationError(
            "output path %s must not be an input file" % output_path
        )
    if cp_abs in input_abs:
        from .errors import ConfigurationError
        raise ConfigurationError(
            "checkpoint path %s must not be an input file" % checkpoint_path
        )


def _ensure_inputs_readable(config):
    for spec in config.sources:
        path = spec["path"]
        if not os.path.exists(path):
            raise SourceError("input %s does not exist" % path)
        if not os.path.isfile(path):
            raise SourceError("input %s is not a regular file" % path)
        if not os.access(path, os.R_OK):
            raise SourceError("input %s is not readable" % path)


def _open_sink_for_run(path):
    parent = os.path.dirname(os.path.abspath(path)) or "."
    if not os.path.isdir(parent):
        raise SinkError("output directory %s does not exist" % parent)
    try:
        return open(path, "xb")
    except FileExistsError:
        from .errors import ConfigurationError
        raise ConfigurationError(
            "output %s already exists; run requires a fresh path" % path
        )
    except OSError as exc:
        raise SinkError("cannot open output %s: %s" % (path, exc))


def _open_sink_for_replay(path, checkpoint):
    if not os.path.exists(path):
        raise CheckpointError(
            "output %s does not exist; cannot recover" % path
        )
    if not os.path.isfile(path):
        raise CheckpointError("output %s is not a regular file" % path)
    try:
        fp = open(path, "r+b")
    except OSError as exc:
        raise SinkError("cannot open output %s: %s" % (path, exc))
    try:
        size = os.fstat(fp.fileno()).st_size
        if size < checkpoint.sink_offset:
            raise CheckpointError(
                "output %s is shorter than the committed checkpoint; "
                "cannot recover" % path
            )
        if size > checkpoint.sink_offset:
            fp.truncate(checkpoint.sink_offset)
            fp.flush()
            os.fsync(fp.fileno())
        fp.seek(0, os.SEEK_END)
    except CheckpointError:
        fp.close()
        raise
    except OSError as exc:
        fp.close()
        raise SinkError("cannot recover output %s: %s" % (path, exc))
    return fp


# --------------------------------------------------------------------------
# Record processing
# --------------------------------------------------------------------------


def _extract_envelope(obj, source_id, line_number):
    where = "source %r line %d" % (source_id, line_number)
    if not isinstance(obj, dict):
        raise DataValidationError("%s: record must be a JSON object" % where)
    for key in ("source_id", "event_id", "payload"):
        if key not in obj:
            raise DataValidationError("%s: record is missing %r" % (where, key))
    if not isinstance(obj["source_id"], str) or obj["source_id"] == "":
        raise DataValidationError("%s: source_id must be a non-empty string" % where)
    event_id = obj["event_id"]
    # event_id is any JSON scalar, including null (null only ever matches
    # null under the de-duplication scalar semantics); arrays and objects
    # are rejected.
    if isinstance(event_id, (dict, list)):
        raise DataValidationError("%s: event_id must be a scalar" % where)
    if not isinstance(obj["payload"], dict):
        raise DataValidationError("%s: payload must be a JSON object" % where)
    return obj["source_id"], event_id, obj["payload"]


def _write_record(sink, record):
    try:
        sink.write((json.dumps(record, ensure_ascii=False) + "\n").encode(ENCODING))
    except (OSError, TypeError, ValueError) as exc:
        raise SinkError("cannot write output: %s" % exc)


def _flush_sink(sink):
    try:
        sink.flush()
        os.fsync(sink.fileno())
    except OSError as exc:
        raise SinkError("cannot flush output: %s" % exc)


# --------------------------------------------------------------------------
# CSV parsing (RFC 4180 subset: comma delimiters, quoted fields, doubled
# quotes, LF/CRLF, embedded newlines, optional leading UTF-8 BOM)
# --------------------------------------------------------------------------


_COMMA = 0x2C
_QUOTE = 0x22
_CR = 0x0D
_LF = 0x0A


class _CSVParser:
    """Streaming byte-oriented parser for one CSV file.

    Structural characters never occur inside UTF-8 multi-byte sequences, so
    the grammar is recognised on raw bytes and individual fields are decoded
    on demand; this keeps the absolute byte offset of every logical record
    boundary exact for checkpointing. Only the bytes of the record currently
    being parsed are buffered.
    """

    CHUNK = 1 << 16

    def __init__(self, source_id, fp, start=0, expect_header=True,
                 data_base=0):
        self.sid = source_id
        self.fp = fp
        self.buf = b""
        self.i = 0
        # Absolute file offset of the next byte to consume.
        self.abs = start
        self.expect_header = expect_header
        # Data records already present before this parser's first record
        # (used on resume so error labels use the file-wide record number).
        self.data_base = data_base
        # 1-based file-wide number of the current/last data record.
        self.data_index = data_base

    # -- low-level byte stream -------------------------------------------

    def _fill(self):
        if self.i >= len(self.buf):
            self.buf = self.fp.read(self.CHUNK)
            self.i = 0
        return self.i < len(self.buf)

    def _peek(self):
        if not self._fill():
            return None
        return self.buf[self.i]

    def _take(self):
        c = self.buf[self.i]
        self.i += 1
        self.abs += 1
        return c

    # -- error helpers ----------------------------------------------------

    def _label(self, is_data):
        if is_data:
            return "source %r record %d" % (self.sid, self.data_index)
        return "source %r header" % self.sid

    def _fail(self, is_data, detail):
        raise DataValidationError("%s: %s" % (self._label(is_data), detail))

    def _decode(self, raw, is_data):
        try:
            text = raw.decode(ENCODING)
        except UnicodeDecodeError:
            self._fail(is_data, "invalid UTF-8")
        if "﻿" in text:
            self._fail(is_data, "BOM is only allowed at the start of the file")
        return text

    # -- parsing ----------------------------------------------------------

    def next_record(self):
        """Parse one logical record.

        Returns ``(raw_bytes, fields, is_header)`` where ``raw_bytes`` are
        the exact input bytes of the record (line terminator included, and
        the leading BOM for a fresh header), or ``None`` at clean EOF.
        """
        c = self._peek()
        if c is None:
            return None
        is_header = self.expect_header
        self.expect_header = False
        is_data = not is_header
        if is_data:
            self.data_index += 1
        raw = bytearray()
        if is_header and self.abs == 0:
            # A BOM is accepted only as the very first bytes of the file; at
            # this point the first chunk is buffered and nothing is consumed.
            self._fill()
            if self.buf[self.i:self.i + len(BOM)] == BOM:
                for _ in range(len(BOM)):
                    self._take()
                raw += BOM
        fields = []
        mode = "start"          # start | unquoted | quoted | after
        field = bytearray()
        quoted_value = None
        while True:
            c = self._peek()
            if mode == "start":
                if c is None:
                    # EOF immediately after a comma: the last field is empty.
                    fields.append("")
                    break
                b = self._take()
                raw.append(b)
                if b == _QUOTE:
                    mode = "quoted"
                elif b == _COMMA:
                    fields.append("")
                elif b == _LF or b == _CR:
                    if not fields:
                        self._fail(is_data, "empty logical record")
                    fields.append("")
                    self._finish_terminator(b, is_data, raw)
                    break
                else:
                    field.append(b)
                    mode = "unquoted"
            elif mode == "unquoted":
                if c is None:
                    fields.append(self._decode(bytes(field), is_data))
                    break
                b = self._take()
                raw.append(b)
                if b == _QUOTE:
                    self._fail(is_data, "unexpected quote in unquoted field")
                if b == _COMMA:
                    fields.append(self._decode(bytes(field), is_data))
                    field = bytearray()
                    mode = "start"
                elif b == _LF or b == _CR:
                    fields.append(self._decode(bytes(field), is_data))
                    self._finish_terminator(b, is_data, raw)
                    break
                else:
                    field.append(b)
            elif mode == "quoted":
                if c is None:
                    self._fail(is_data, "unterminated quoted field")
                b = self._take()
                raw.append(b)
                if b == _QUOTE:
                    if self._peek() == _QUOTE:
                        # Escaped quote: keep both bytes; collapse on decode.
                        field.append(b)
                        field.append(self._take())
                        raw.append(_QUOTE)
                    else:
                        quoted_value = self._decode(
                            bytes(field).replace(b'""', b'"'), is_data)
                        mode = "after"
                else:
                    field.append(b)
            else:  # directly after a closing quote
                if c is None:
                    fields.append(quoted_value)
                    break
                b = self._take()
                raw.append(b)
                if b == _COMMA:
                    fields.append(quoted_value)
                    field = bytearray()
                    mode = "start"
                elif b == _LF or b == _CR:
                    fields.append(quoted_value)
                    self._finish_terminator(b, is_data, raw)
                    break
                else:
                    self._fail(is_data, "unexpected text after quoted field")
        return bytes(raw), fields, is_header

    def _finish_terminator(self, first, is_data, raw):
        if first == _CR:
            nxt = self._peek()
            if nxt != _LF:
                self._fail(is_data,
                           "bare carriage return is not a line terminator")
            raw.append(self._take())


def _open_input(spec):
    try:
        return open(spec["path"], "rb")
    except OSError as exc:
        raise SourceError("cannot open input %s: %s" % (spec["path"], exc))


def _verify_committed_prefix(spec, state):
    """Verify the committed input prefix, then position at its boundary.

    Re-hashes the raw input bytes ``[0:state.offset]`` and compares them
    against the checkpointed hash, also rejecting a file that has shrunk
    below the committed boundary. Returns ``(fp, hasher)`` with ``fp``
    positioned at the resume offset and ``hasher`` already covering the
    committed prefix, so later updates extend it to the new prefix.
    """
    fp = _open_input(spec)
    target = state.offset
    hasher = hashlib.sha256()
    try:
        size = os.fstat(fp.fileno()).st_size
        if target > size:
            raise CheckpointError(
                "input %s is shorter than the committed offset; "
                "cannot recover" % spec["path"]
            )
        remaining = target
        while remaining > 0:
            try:
                chunk = fp.read(min(1 << 16, remaining))
            except OSError as exc:
                raise SourceError(
                    "cannot read %s: %s" % (spec["path"], exc))
            if not chunk:
                break
            hasher.update(chunk)
            remaining -= len(chunk)
        if remaining != 0 or hasher.hexdigest() != state.input_hash:
            raise CheckpointError(
                "committed input prefix for source %r has changed; "
                "cannot recover" % spec["id"]
            )
        try:
            fp.seek(target)
        except OSError as exc:
            raise SourceError("cannot read %s: %s" % (spec["path"], exc))
    except CheckpointError:
        fp.close()
        raise
    return fp, hasher


def _validate_csv_header(source_id, fields):
    """Validate a CSV header and return the full, ordered column list.

    Column names must be unique and non-empty and include both control
    columns ``source_id`` and ``event_id``.
    """
    if not fields:
        raise DataValidationError("source %r header: empty header" % source_id)
    seen = set()
    for name in fields:
        if name == "":
            raise DataValidationError(
                "source %r header: empty column name" % source_id)
        if name in seen:
            raise DataValidationError(
                "source %r header: duplicate column %r" % (source_id, name))
        seen.add(name)
    for required in ("source_id", "event_id"):
        if required not in seen:
            raise DataValidationError(
                "source %r header: missing required column %r"
                % (source_id, required))
    return list(fields)


def _read_csv_header(spec):
    """Read and validate just the header (used when resuming a CSV source)."""
    fp = _open_input(spec)
    try:
        parser = _CSVParser(spec["id"], fp, start=0, expect_header=True)
        result = parser.next_record()
        if result is None:
            raise DataValidationError(
                "source %r: header is missing" % spec["id"])
        _raw, fields, _is_header = result
        return _validate_csv_header(spec["id"], fields)
    finally:
        fp.close()


def _csv_payload(spec, columns, fields, record_index):
    """Split one CSV data record into control values and the flat payload."""
    where = "source %r record %d" % (spec["id"], record_index)
    if len(fields) != len(columns):
        raise DataValidationError(
            "%s: expected %d columns, got %d"
            % (where, len(columns), len(fields)))
    sid_index = columns.index("source_id")
    eid_index = columns.index("event_id")
    source_id = fields[sid_index]
    event_id = fields[eid_index]
    if source_id != spec["id"]:
        raise DataValidationError(
            "%s: source_id %r does not match configured source"
            % (where, source_id))
    if event_id == "":
        raise DataValidationError(
            "%s: event_id must be a non-empty string" % where)
    payload = {
        columns[j]: fields[j]
        for j in range(len(columns))
        if j != sid_index and j != eid_index
    }
    return source_id, event_id, payload


def _process(config, states, sink, checkpoint_path):
    """Process every source from its current offset; commit per batch."""
    pending = 0

    def commit():
        _flush_sink(sink)
        sink_offset = os.fstat(sink.fileno()).st_size
        Checkpoint(config, states, sink_offset).write_atomic(checkpoint_path)

    counts = {}
    for spec, state in zip(config.sources, states):
        records_before = state.records
        resuming = state.offset > 0
        if resuming:
            fp, hasher = _verify_committed_prefix(spec, state)
        else:
            fp = _open_input(spec)
            hasher = hashlib.sha256()
        try:
            if spec["type"] == "jsonl":
                pending = _process_jsonl(
                    spec, state, config, sink, commit, pending, fp, hasher)
            else:
                pending = _process_csv(
                    spec, state, config, sink, commit, pending, fp, hasher,
                    resuming)
        finally:
            fp.close()
        counts[spec["id"]] = state.records - records_before
    # A successful invocation always leaves a checkpoint on disk, even when
    # there was nothing new to append.
    commit()
    return counts


def _commit_boundary(state, boundary, hasher, commit):
    """Advance the durable input boundary and atomically commit."""
    state.offset = boundary
    state.input_hash = hasher.hexdigest()
    commit()


def _emit_record(spec, state, config, sink, source_id, event_id, payload,
                 where):
    # De-duplication runs after envelope validation and before any
    # transform: a duplicate id skips transforms entirely.
    duplicate = state.is_duplicate(event_id)
    data = None
    if not duplicate:
        data = apply_transforms(payload, config.transforms, where)
        # Source-level transforms run after the shared ones, in their own
        # configured order.
        if data is not None:
            data = apply_transforms(data, spec["transforms"], where)
    # Every consumed input record counts toward the batch and the committed
    # prefix and slides the window -- first occurrence, duplicate and
    # filtered alike -- so a filtered first occurrence still hides its
    # successors. Duplicates and filtered records produce no output and no
    # schema change. (A failing transform raises before this point, so the
    # uncommitted record is not counted or windowed.)
    state.records += 1
    state.observe(event_id)
    if duplicate or data is None:
        return
    state.note_schema(schema_fingerprint(data))
    record = {
        "source_id": source_id,
        "event_id": event_id,
        "schema_version": state.schema_version,
        "data": data,
    }
    _write_record(sink, record)


def _process_jsonl(spec, state, config, sink, commit, pending, fp, hasher):
    boundary = state.offset
    # state.records counts every consumed input line (filtered or not), so
    # it doubles as the number of lines already committed.
    line_number = state.records
    while True:
        try:
            raw_line = fp.readline()
        except OSError as exc:
            raise SourceError("cannot read %s: %s" % (spec["path"], exc))
        if not raw_line:
            break
        line_number += 1
        boundary += len(raw_line)
        hasher.update(raw_line)
        # Every line of a jsonl source is a JSON object; blank or
        # whitespace-only lines are records that fail validation.
        where = "source %r line %d" % (spec["id"], line_number)
        try:
            text = raw_line.decode(ENCODING)
        except UnicodeDecodeError:
            raise DataValidationError("%s: malformed JSON" % where)
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            raise DataValidationError("%s: malformed JSON" % where)
        source_id, event_id, payload = _extract_envelope(
            obj, spec["id"], line_number)
        if source_id != spec["id"]:
            raise DataValidationError(
                "%s: source_id %r does not match configured source"
                % (where, source_id))
        _emit_record(
            spec, state, config, sink, source_id, event_id, payload, where)
        pending += 1
        if pending >= spec["batch_size"]:
            _commit_boundary(state, boundary, hasher, commit)
            pending = 0
    if pending > 0:
        _commit_boundary(state, boundary, hasher, commit)
        pending = 0
    return pending


def _process_csv(spec, state, config, sink, commit, pending, fp, hasher,
                 resuming):
    if resuming:
        columns = _read_csv_header(spec)
        parser = _CSVParser(
            spec["id"], fp, start=state.offset, expect_header=False,
            data_base=state.records)
    else:
        parser = _CSVParser(spec["id"], fp, start=0, expect_header=True)
        result = parser.next_record()
        if result is None:
            raise DataValidationError(
                "source %r: header is missing" % spec["id"])
        raw_header, fields, _is_header = result
        columns = _validate_csv_header(spec["id"], fields)
        # The header is part of the source's committed input prefix; make
        # its boundary durable immediately so even a header-only file
        # resumes correctly.
        hasher.update(raw_header)
        _commit_boundary(state, parser.abs, hasher, commit)
    while True:
        result = parser.next_record()
        if result is None:
            break
        raw_record, fields, _is_header = result
        hasher.update(raw_record)
        source_id, event_id, payload = _csv_payload(
            spec, columns, fields, parser.data_index)
        where = "source %r record %d" % (spec["id"], parser.data_index)
        _emit_record(
            spec, state, config, sink, source_id, event_id, payload, where)
        pending += 1
        if pending >= spec["batch_size"]:
            _commit_boundary(state, parser.abs, hasher, commit)
            pending = 0
    if pending > 0:
        _commit_boundary(state, parser.abs, hasher, commit)
        pending = 0
    return pending


# --------------------------------------------------------------------------
# Public entry points
# --------------------------------------------------------------------------


def run(config, output_path, checkpoint_path):
    """Fresh run: neither output nor checkpoint may exist beforehand."""
    from .errors import ConfigurationError

    _guard_paths(config, output_path, checkpoint_path)
    if os.path.exists(checkpoint_path):
        raise ConfigurationError(
            "checkpoint %s already exists; run requires a fresh path"
            % checkpoint_path
        )
    cp_parent = os.path.dirname(os.path.abspath(checkpoint_path)) or "."
    if not os.path.isdir(cp_parent):
        raise SinkError("checkpoint directory %s does not exist" % cp_parent)
    _ensure_inputs_readable(config)
    states = [SourceState(spec) for spec in config.sources]
    sink = _open_sink_for_run(output_path)
    try:
        return _process(config, states, sink, checkpoint_path)
    finally:
        sink.close()


def replay(config, output_path, checkpoint_path):
    """Resume after the last committed batch, appending without dupes/gaps."""
    _guard_paths(config, output_path, checkpoint_path)
    if not os.path.exists(checkpoint_path):
        raise CheckpointError(
            "checkpoint %s does not exist; replay requires it" % checkpoint_path
        )
    checkpoint = load_checkpoint(checkpoint_path, config)
    _ensure_inputs_readable(config)
    sink = _open_sink_for_replay(output_path, checkpoint)
    try:
        return _process(config, checkpoint.states, sink, checkpoint_path)
    finally:
        sink.close()
