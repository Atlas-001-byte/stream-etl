"""The ETL engine: run, replay, transforms, schema evolution, checkpoints.

For every input record (a JSON object per line for ``jsonl`` sources, a
logical row for ``csv`` sources) the engine reads ``source_id`` /
``event_id`` and the payload, applies the configured transforms to the
payload in order, and emits one output object::

    {"source_id": ..., "event_id": ..., "schema_version": N, "data": {...}}

Sources are processed in configuration order. Every ``batch_size`` records
for a source (and at each source boundary) the output is fsynced and the
checkpoint is atomically replaced. The checkpoint stores, per source, the
byte offset of the next logical record and the SHA-256 of the committed
input prefix. Replay verifies that prefix, truncates any uncommitted tail
of the output using the byte offset stored in the checkpoint, then
continues, so records are never duplicated or skipped.
"""

import hashlib
import json
import os
import tempfile

from .config import split_path
from .csv_source import iter_csv_records, prepare_csv_header
from .errors import (
    CheckpointError,
    DataValidationError,
    SinkError,
    SourceError,
)

CHECKPOINT_VERSION = 1
ENCODING = "utf-8"
EMPTY_INPUT_DIGEST = hashlib.sha256(b"").hexdigest()


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
# Transforms
# --------------------------------------------------------------------------


def apply_transforms(data, transforms, where):
    """Apply transforms in order to ``data`` (mutated in place)."""
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
        self.offset = 0
        self.records = 0
        self.schema_version = 0
        self.schema_fingerprint = None
        # SHA-256 (hex) of the committed input prefix; ``_hasher`` is the
        # running hash armed once the prefix has been verified.
        self.input_digest = EMPTY_INPUT_DIGEST
        self._hasher = None

    def note_schema(self, fp):
        if fp != self.schema_fingerprint:
            self.schema_version += 1
            self.schema_fingerprint = fp

    def arm_hasher(self, hasher):
        self._hasher = hasher

    def note_bytes(self, data):
        if self._hasher is None:
            self._hasher = hashlib.sha256()
        self._hasher.update(data)

    def to_json(self):
        return {
            "path": self.spec["path"],
            "offset": self.offset,
            "records": self.records,
            "schema_version": self.schema_version,
            "schema_fingerprint": self.schema_fingerprint,
            "input_digest": (self._hasher.hexdigest()
                             if self._hasher is not None
                             else self.input_digest),
        }

    @classmethod
    def from_json(cls, spec, raw):
        state = cls(spec)
        if not isinstance(raw, dict):
            raise CheckpointError("checkpoint entry for source %r is invalid"
                                  % spec["id"])
        required = ("path", "offset", "records", "schema_version",
                    "schema_fingerprint", "input_digest")
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
        fp = raw["schema_fingerprint"]
        if fp is not None and not isinstance(fp, str):
            raise CheckpointError(
                "checkpoint schema_fingerprint for source %r is invalid"
                % spec["id"]
            )
        digest = raw["input_digest"]
        if not isinstance(digest, str) or len(digest) != 64:
            raise CheckpointError(
                "checkpoint input_digest for source %r is invalid"
                % spec["id"]
            )
        state.offset = raw["offset"]
        state.records = raw["records"]
        state.schema_version = raw["schema_version"]
        state.schema_fingerprint = fp
        state.input_digest = digest
        return state


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
    if event_id is None or isinstance(event_id, (dict, list)):
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


def _count_lines_before(path, offset):
    if offset == 0:
        return 0
    try:
        with open(path, "rb") as fp:
            return sum(1 for _ in fp.read(offset).splitlines())
    except OSError as exc:
        raise SourceError("cannot read %s: %s" % (path, exc))


def _verify_input_prefix(fp, spec, state):
    """The committed input prefix must be byte-identical to what the
    checkpoint recorded; arm the running digest for the new records."""
    hasher = hashlib.sha256()
    remaining = state.offset
    fp.seek(0)
    while remaining > 0:
        try:
            chunk = fp.read(min(65536, remaining))
        except OSError as exc:
            raise SourceError("cannot read %s: %s" % (spec["path"], exc))
        if not chunk:
            break
        hasher.update(chunk)
        remaining -= len(chunk)
    if hasher.hexdigest() != state.input_digest:
        raise CheckpointError(
            "input %s committed prefix changed; cannot recover"
            % spec["path"]
        )
    state.arm_hasher(hasher)


def _jsonl_records(fp, spec, state):
    """Yield (where, source_id, event_id, payload) for each remaining line."""
    fp.seek(state.offset)
    line_number = _count_lines_before(spec["path"], state.offset)
    while True:
        try:
            raw_line = fp.readline()
        except OSError as exc:
            raise SourceError("cannot read %s: %s" % (spec["path"], exc))
        if not raw_line:
            return
        state.offset += len(raw_line)
        state.note_bytes(raw_line)
        line_number += 1
        try:
            text = raw_line.decode(ENCODING)
        except UnicodeDecodeError:
            raise DataValidationError(
                "source %r line %d: malformed JSON"
                % (spec["id"], line_number)
            )
        # Every line of a jsonl source is a JSON object; blank or
        # whitespace-only lines are records that fail validation.
        where = "source %r line %d" % (spec["id"], line_number)
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            raise DataValidationError("%s: malformed JSON" % where)
        source_id, event_id, payload = _extract_envelope(
            obj, spec["id"], line_number
        )
        if source_id != spec["id"]:
            raise DataValidationError(
                "%s: source_id %r does not match configured source"
                % (where, source_id)
            )
        yield where, source_id, event_id, payload


def _source_records(fp, spec, state):
    """Record iterator for the source type, positioned after the committed
    prefix (for csv: after the header's logical record boundary)."""
    if spec["type"] == "csv":
        header = prepare_csv_header(fp, spec, state)
        if header is None:
            return iter(())
        return iter_csv_records(fp, spec, state, header)
    return _jsonl_records(fp, spec, state)


def _process(config, states, sink, checkpoint_path):
    """Process every source from its current offset; commit per batch."""
    pending = 0

    def commit():
        _flush_sink(sink)
        sink_offset = os.fstat(sink.fileno()).st_size
        Checkpoint(config, states, sink_offset).write_atomic(checkpoint_path)

    counts = {}
    for spec, state in zip(config.sources, states):
        emitted = 0
        try:
            fp = open(spec["path"], "rb")
        except OSError as exc:
            raise SourceError("cannot open input %s: %s" % (spec["path"], exc))
        try:
            size = os.fstat(fp.fileno()).st_size
            if state.offset > size:
                raise CheckpointError(
                    "input %s is shorter than the committed offset; "
                    "cannot recover" % spec["path"]
                )
            _verify_input_prefix(fp, spec, state)
            records = _source_records(fp, spec, state)
            for where, source_id, event_id, payload in records:
                data = apply_transforms(payload, config.transforms, where)
                # Source-level transforms run after the shared ones, in
                # their own configured order.
                data = apply_transforms(data, spec["transforms"], where)
                state.note_schema(schema_fingerprint(data))
                record = {
                    "source_id": source_id,
                    "event_id": event_id,
                    "schema_version": state.schema_version,
                    "data": data,
                }
                _write_record(sink, record)
                state.records += 1
                emitted += 1
                pending += 1
                if pending >= spec["batch_size"]:
                    commit()
                    pending = 0
        finally:
            fp.close()
        if pending > 0:
            commit()
            pending = 0
        counts[spec["id"]] = emitted
    # A successful invocation always leaves a checkpoint on disk, even when
    # there was nothing new to append.
    commit()
    return counts


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
