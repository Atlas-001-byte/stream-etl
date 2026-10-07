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
object is emitted per surviving branch::

    {"source_id": ..., "event_id": ..., "schema_version": N, "data": {...}}

A ``filter`` transform instead decides whether the record survives: when its
condition fails the input record is still consumed (it counts toward the
batch and the committed prefix) but emits no output and no schema change.

An ``explode`` transform fans one record out into several branches: the
array at its dotted field path is removed and each of its elements (which
must be a JSON object) is merged with the array's former sibling fields to
form one branch, in array order. Every branch then continues through the
remaining transforms independently, so one input record may emit zero, one
or many output records, all sharing the input's ``event_id``; an empty
array emits none but is still consumed like a filtered record.

A source may configure ``dedup_window: N`` (a positive integer). The source
then keeps the ``event_id`` values of the last ``N - 1`` *consumed* input
records in a sliding window. Right after envelope validation and before any
transform runs, a record whose ``event_id`` matches a windowed key under JSON
scalar semantics is treated as a duplicate: it is consumed (it counts toward
the batch and the committed prefix, and slides through the window) but emits
no output and triggers no schema change. ``N == 1`` keeps an empty window, so
nothing is ever deduplicated; a source without ``dedup_window`` behaves
exactly as before. Windows are per source and never interact.

A source may instead (or additionally) configure an event-time watermark via
the triple ``event_time`` / ``watermark_delay`` / ``late_policy``. The source
then tracks the maximum event time seen so far and judges every record —
right after envelope validation, before dedup and any transform — against
the watermark ``max_event_time - watermark_delay`` as it stood *before* the
record arrived: a record whose ``event_time`` value (a finite non-boolean
number read from the pre-transform payload) is strictly earlier than that
watermark is late. ``late_policy: drop`` consumes the late record (it counts
toward the batch and the committed prefix and slides through the dedup
window) without emitting output, changing the schema version or raising the
maximum; ``late_policy: error`` fails the record with a DataValidationError.
A non-late record's event time joins the maximum even when the record is
later deduplicated, filtered out or exploded into zero branches. The maximum
and the watermark are persisted with every committed boundary and restored
on replay.

A source may configure ``schema_policy: compatible`` (the default,
``allow``, keeps the historical behaviour). Under ``compatible`` every
record the source actually emits — after the shared and source-level
transforms ran in their configured order — is judged in emission order
against the source's compatibility baseline. The first emitted record
establishes the baseline; afterwards an object may only gain fields, while
removing a field, retyping an existing field between
string/integer/number/boolean/null, changing a value between object, array
and scalar, or changing the element shape set of an array is an
incompatible change and fails the record (and its batch) with a
DataValidationError. Compatible changes keep bumping ``schema_version``
under the usual rules; filtered records, empty arrays and dropped branches
never take part in the judgement. The baseline is persisted with every
committed boundary (checkpoint version 5) and validated on replay.

Sources are processed in configuration order. Every ``batch_size`` records
(and at each source boundary) the output is fsynced and the checkpoint is
atomically replaced. The checkpoint stores the byte offset of the last
committed record boundary together with a hash of the input prefix up to it
and, for dedup sources, the complete sliding window; replay verifies that
prefix is unchanged and the file has not shrunk, truncates any uncommitted
tail of the output using the sink byte offset, then continues, so records
are never duplicated or skipped.
"""

import copy
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
# Version 3 adds the per-source dedup sliding window. It is only used by
# configurations that set ``dedup_window``; plain configurations keep writing
# version 2 and old version 2 checkpoints remain replayable against them.
CHECKPOINT_VERSION_DEDUP = 3
# Version 4 adds the per-source event-time watermark state. It is only used
# by configurations that set the watermark triple; configurations without it
# keep writing versions 2/3 and those checkpoints remain replayable.
CHECKPOINT_VERSION_WATERMARK = 4
# Version 5 adds the per-source compatibility baseline used by
# ``schema_policy: compatible``. It is only used by configurations that set
# the policy to ``compatible``; configurations without it (including an
# explicit ``allow``) keep writing versions 2/3/4 and those checkpoints
# remain replayable.
CHECKPOINT_VERSION_SCHEMA_POLICY = 5
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


def _apply_one(data, raw, where):
    """Apply one non-explode transform to a single branch (mutated in place).

    Returns the transformed data, or ``None`` when a ``filter`` transform
    drops the branch.
    """
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


def _explode(data, parts, where):
    """Fan one branch out into one branch per element of the array at ``parts``.

    Each element must be a JSON object; its keys are merged with the fields
    that shared the array's level (the array field itself is removed), and a
    key colliding with one of those siblings is a data error. Every branch
    is a fully independent copy, so later transforms on one branch cannot
    leak into another. An empty array yields zero branches.
    """
    parent, key = _parent_and_key(data, parts, where)
    value = parent[key]
    if not isinstance(value, list):
        raise DataValidationError(
            "%s: field %r is not an array" % (where, _desc(parts)))
    siblings = set(parent) - {key}
    branches = []
    for index, element in enumerate(value):
        if not isinstance(element, dict):
            raise DataValidationError(
                "%s: element %d of %r is not an object"
                % (where, index, _desc(parts)))
        conflict = sorted(set(element) & siblings)
        if conflict:
            raise DataValidationError(
                "%s: element key %r of %r conflicts with an existing field"
                % (where, conflict[0], _desc(parts)))
        branch = copy.deepcopy(data)
        branch_parent, branch_key = _parent_and_key(branch, parts, where)
        merged = branch_parent[branch_key][index]
        del branch_parent[branch_key]
        branch_parent.update(merged)
        branches.append(branch)
    return branches


def apply_pipeline(datas, transforms, where):
    """Apply transforms in order to every branch in ``datas``.

    Returns the surviving branches in stable (array, then configuration)
    order: ``filter`` drops its branch, ``explode`` replaces its branch by
    one branch per array element, and every other transform maps one branch
    to one branch.
    """
    branches = list(datas)
    for raw in transforms:
        if raw["op"] == "explode":
            parts = split_path(raw["field"])
            branches = [
                branch
                for data in branches
                for branch in _explode(data, parts, where)
            ]
        else:
            branches = [
                result
                for result in (_apply_one(data, raw, where)
                               for data in branches)
                if result is not None
            ]
        if not branches:
            break
    return branches


def apply_transforms(data, transforms, where):
    """Apply transforms in order to a single ``data`` (mutated in place).

    Returns the transformed data, or ``None`` when a ``filter`` transform
    drops the record (the input record then produces no output). When an
    ``explode`` transform fans the record out into several branches, the
    list of branches is returned instead.
    """
    branches = apply_pipeline([data], transforms, where)
    if len(branches) == 1:
        return branches[0]
    if not branches:
        return None
    return branches


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


def _shape_extends(base, new):
    """True when ``new`` is a compatible evolution of the ``base`` shape.

    Both arguments are decoded ``_schema_shape`` descriptors. An object may
    only gain fields (every baseline field must still be present, itself a
    compatible evolution); a scalar type tag must stay identical; an
    array's element shape set must be exactly the same; and any change
    between object, array and scalar is incompatible.
    """
    if isinstance(base, str) or isinstance(new, str):
        return base == new
    if "dict" in base and "dict" in new:
        return all(
            key in new["dict"] and _shape_extends(sub, new["dict"][key])
            for key, sub in base["dict"].items()
        )
    if "list" in base and "list" in new:
        return base["list"] == new["list"]
    return False


# --------------------------------------------------------------------------
# Per-source state and checkpoint (de)serialisation
# --------------------------------------------------------------------------


class SourceState:
    def __init__(self, spec):
        self.spec = spec
        self.offset = 0
        self.input_hash = EMPTY_PREFIX_HASH
        self.records = 0
        self.schema_version = 0
        self.schema_fingerprint = None
        # ``dedup_window`` is None for sources without dedup (legacy
        # behaviour); otherwise the window holds the event_id values of the
        # last ``dedup_window - 1`` consumed input records.
        self.dedup_window = spec.get("dedup_window")
        if self.dedup_window is None:
            self.window = None
        else:
            self.window = deque((), maxlen=self.dedup_window - 1)
        # Event-time watermark state: None/absent when the source does not
        # configure the watermark triple (legacy behaviour). ``max_event_time``
        # is the largest event time of any non-late consumed record; the
        # watermark is that maximum minus ``watermark_delay``.
        self.event_time_path = spec.get("event_time")
        if self.event_time_path is None:
            self.event_time_parts = None
            self.watermark_delay = None
            self.late_policy = None
        else:
            self.event_time_parts = split_path(self.event_time_path)
            self.watermark_delay = spec["watermark_delay"]
            self.late_policy = spec["late_policy"]
        self.max_event_time = None
        # ``schema_policy`` is None for sources without the policy (or with
        # an explicit ``allow``: the legacy behaviour); ``compatible``
        # constrains how the emitted ``data`` shapes may evolve.
        self.schema_policy = spec.get("schema_policy")

    def watermark(self):
        """Current watermark, or None before the first consumed record."""
        if self.max_event_time is None:
            return None
        return self.max_event_time - self.watermark_delay

    def is_late(self, event_time):
        """True when ``event_time`` falls before the incoming watermark.

        The first record is never late (no watermark exists yet) and an
        event time exactly equal to the watermark is not late.
        """
        watermark = self.watermark()
        return watermark is not None and event_time < watermark

    def note_event_time(self, event_time):
        """Fold one non-late record's event time into the maximum."""
        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time

    def note_schema(self, fp):
        if fp != self.schema_fingerprint:
            self.schema_version += 1
            self.schema_fingerprint = fp

    def check_schema_compatible(self, fp, where):
        """Enforce ``schema_policy: compatible`` for one actual output.

        The first emitted record establishes the compatibility baseline;
        every later shape must extend it (objects may only gain fields).
        The baseline is exactly the shape of the last accepted output, so
        ``schema_fingerprint`` doubles as the persisted baseline.
        """
        if self.schema_fingerprint is None:
            return
        base = json.loads(self.schema_fingerprint)
        new = json.loads(fp)
        if not _shape_extends(base, new):
            raise DataValidationError(
                "%s: schema_policy 'compatible' forbids this schema "
                "change (fields may only be added)" % where)

    def note_event(self, event_id):
        """Register one consumed input record; return True if duplicated.

        The key is compared against the window under JSON scalar semantics;
        every consumed record (first occurrence, duplicate or filtered-out)
        slides through the window. With no dedup configured nothing is ever
        reported as a duplicate; with ``dedup_window == 1`` the window is
        empty, so the same holds.
        """
        if self.window is None:
            return False
        duplicate = any(
            _scalar_equal(event_id, key) for key in self.window
        )
        self.window.append(event_id)
        return duplicate

    def to_json(self):
        doc = {
            "path": self.spec["path"],
            "offset": self.offset,
            "input_hash": self.input_hash,
            "records": self.records,
            "schema_version": self.schema_version,
            "schema_fingerprint": self.schema_fingerprint,
        }
        if self.dedup_window is not None:
            doc["dedup_window"] = self.dedup_window
            doc["dedup_keys"] = list(self.window)
        if self.event_time_path is not None:
            doc["event_time"] = self.event_time_path
            doc["watermark_delay"] = self.watermark_delay
            doc["late_policy"] = self.late_policy
            doc["max_event_time"] = self.max_event_time
            doc["watermark"] = self.watermark()
        if self.schema_policy == "compatible":
            doc["schema_policy"] = self.schema_policy
            # The compatibility baseline is the shape of the last accepted
            # output; it is stored alongside (and must agree with) the
            # schema fingerprint so a corrupt or foreign checkpoint is
            # detected on replay.
            doc["schema_baseline"] = self.schema_fingerprint
        return doc

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
        state.window = state._restore_window(spec, raw)
        state._restore_watermark(spec, raw)
        state._restore_schema_policy(spec, raw)
        return state

    def _restore_schema_policy(self, spec, raw):
        """Validate the persisted compatibility baseline, if configured."""
        configured = spec.get("schema_policy")
        has_state = "schema_policy" in raw or "schema_baseline" in raw
        if configured is None:
            if has_state:
                raise CheckpointError(
                    "checkpoint for source %r carries schema policy state "
                    "but the source configures no schema_policy"
                    % spec["id"]
                )
            return
        for key in ("schema_policy", "schema_baseline"):
            if key not in raw:
                raise CheckpointError(
                    "checkpoint for source %r is missing the schema "
                    "policy state %r" % (spec["id"], key)
                )
        if raw["schema_policy"] != configured:
            raise CheckpointError(
                "checkpoint schema_policy for source %r differs from the "
                "configuration" % spec["id"]
            )
        baseline = raw["schema_baseline"]
        if baseline is not None and not isinstance(baseline, str):
            raise CheckpointError(
                "checkpoint schema_baseline for source %r is invalid"
                % spec["id"]
            )
        # The baseline is the shape of the last accepted output, so it
        # must agree with the recorded fingerprint; a source with no
        # output yet has no baseline and version 0, and a source that has
        # consumed no records cannot have emitted anything.
        if baseline != self.schema_fingerprint:
            raise CheckpointError(
                "checkpoint schema_baseline for source %r contradicts "
                "its schema_fingerprint" % spec["id"]
            )
        if (baseline is None) != (self.schema_version == 0):
            raise CheckpointError(
                "checkpoint schema_baseline for source %r is inconsistent "
                "with its schema_version" % spec["id"]
            )
        if self.records == 0 and baseline is not None:
            raise CheckpointError(
                "checkpoint schema_baseline for source %r is inconsistent "
                "with its record count" % spec["id"]
            )

    def _restore_watermark(self, spec, raw):
        """Validate and rebuild the watermark state from checkpoint data."""
        configured = spec.get("event_time") is not None
        has_wm_state = any(
            key in raw
            for key in ("event_time", "watermark_delay", "late_policy",
                        "max_event_time", "watermark")
        )
        if not configured:
            if has_wm_state:
                raise CheckpointError(
                    "checkpoint for source %r carries watermark state but "
                    "the source configures no event_time" % spec["id"]
                )
            return
        for key in ("event_time", "watermark_delay", "late_policy",
                    "max_event_time", "watermark"):
            if key not in raw:
                raise CheckpointError(
                    "checkpoint for source %r is missing the watermark "
                    "state %r" % (spec["id"], key)
                )
        for key in ("event_time", "watermark_delay", "late_policy"):
            if raw[key] != spec[key]:
                raise CheckpointError(
                    "checkpoint %s for source %r differs from the "
                    "configuration" % (key, spec["id"])
                )
        max_event_time = raw["max_event_time"]
        watermark = raw["watermark"]
        for key, value in (("max_event_time", max_event_time),
                           ("watermark", watermark)):
            if value is not None and not is_finite_number(value):
                raise CheckpointError(
                    "checkpoint %s for source %r is invalid"
                    % (key, spec["id"])
                )
        # The first consumed record is never late, so a source that has
        # consumed records always has a maximum (and vice versa).
        if (max_event_time is None) != (self.records == 0):
            raise CheckpointError(
                "checkpoint max_event_time for source %r is inconsistent "
                "with its record count" % spec["id"]
            )
        if (watermark is None) != (max_event_time is None) or (
            max_event_time is not None
            and watermark != max_event_time - self.watermark_delay
        ):
            raise CheckpointError(
                "checkpoint watermark for source %r is inconsistent with "
                "its max_event_time and watermark_delay" % spec["id"]
            )
        self.max_event_time = max_event_time

    def _restore_window(self, spec, raw):
        """Validate and rebuild the dedup window from checkpoint data."""
        configured = spec.get("dedup_window")
        has_window_state = "dedup_window" in raw or "dedup_keys" in raw
        if configured is None:
            if has_window_state:
                raise CheckpointError(
                    "checkpoint for source %r carries dedup window state "
                    "but the source configures no dedup_window"
                    % spec["id"]
                )
            return None
        if "dedup_window" not in raw or "dedup_keys" not in raw:
            raise CheckpointError(
                "checkpoint for source %r is missing the dedup window "
                "state" % spec["id"]
            )
        saved_window = raw["dedup_window"]
        if (isinstance(saved_window, bool)
                or not isinstance(saved_window, int)
                or saved_window < 1):
            raise CheckpointError(
                "checkpoint dedup_window for source %r is invalid"
                % spec["id"]
            )
        if saved_window != configured:
            raise CheckpointError(
                "checkpoint dedup_window for source %r is %d but the "
                "configuration is %d"
                % (spec["id"], saved_window, configured)
            )
        keys = raw["dedup_keys"]
        if not isinstance(keys, list):
            raise CheckpointError(
                "checkpoint dedup window for source %r is invalid"
                % spec["id"]
            )
        capacity = configured - 1
        if len(keys) > capacity:
            raise CheckpointError(
                "checkpoint dedup window for source %r exceeds its "
                "configured window" % spec["id"]
            )
        # Every consumed record slides through the window, so its size is
        # exactly the consumed-record count capped at N-1; a mismatch means
        # the state is corrupt or was produced by a different stream.
        if len(keys) != min(self.records, capacity):
            raise CheckpointError(
                "checkpoint dedup window for source %r is inconsistent "
                "with its record count" % spec["id"]
            )
        for key in keys:
            if isinstance(key, (dict, list)):
                raise CheckpointError(
                    "checkpoint dedup window for source %r contains a "
                    "non-scalar event_id" % spec["id"]
                )
        return deque(keys, maxlen=capacity)


class Checkpoint:
    def __init__(self, config, states, sink_offset):
        self.config = config
        self.states = states
        self.sink_offset = sink_offset

    def to_json(self):
        uses_policy = any(
            spec.get("schema_policy") == "compatible"
            for spec in self.config.sources
        )
        uses_watermark = any(
            spec.get("event_time") is not None
            for spec in self.config.sources
        )
        uses_dedup = any(
            spec.get("dedup_window") is not None
            for spec in self.config.sources
        )
        if uses_policy:
            version = CHECKPOINT_VERSION_SCHEMA_POLICY
        elif uses_watermark:
            version = CHECKPOINT_VERSION_WATERMARK
        elif uses_dedup:
            version = CHECKPOINT_VERSION_DEDUP
        else:
            version = CHECKPOINT_VERSION
        return {
            "version": version,
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
    version = doc.get("version")
    if version not in (CHECKPOINT_VERSION, CHECKPOINT_VERSION_DEDUP,
                       CHECKPOINT_VERSION_WATERMARK,
                       CHECKPOINT_VERSION_SCHEMA_POLICY):
        raise CheckpointError("checkpoint version mismatch (expected %d, got %r)"
                              % (CHECKPOINT_VERSION_SCHEMA_POLICY, version))
    if doc.get("config_fingerprint") != config.fingerprint():
        raise CheckpointError("checkpoint was written by a different configuration")
    any_dedup = any(
        spec.get("dedup_window") is not None for spec in config.sources
    )
    any_watermark = any(
        spec.get("event_time") is not None for spec in config.sources
    )
    any_compatible = any(
        spec.get("schema_policy") == "compatible" for spec in config.sources
    )
    # Versions 2-4 predate schema policies, and a version 5 checkpoint must
    # be backed by at least one compatible-policy source.
    if version < CHECKPOINT_VERSION_SCHEMA_POLICY and any_compatible:
        raise CheckpointError(
            "checkpoint predates schema_policy; cannot recover the "
            "compatibility baseline"
        )
    if version == CHECKPOINT_VERSION_SCHEMA_POLICY and not any_compatible:
        raise CheckpointError(
            "checkpoint carries schema policy state but the configuration "
            "sets no schema_policy"
        )
    # Versions 2/3 predate watermarks, and a version 4 checkpoint must be
    # backed by at least one watermark-configured source.
    if version < CHECKPOINT_VERSION_WATERMARK and any_watermark:
        raise CheckpointError(
            "checkpoint predates event-time watermarks; cannot recover "
            "watermark state"
        )
    if version == CHECKPOINT_VERSION_WATERMARK and not any_watermark:
        raise CheckpointError(
            "checkpoint carries watermark state but the configuration sets "
            "no event_time"
        )
    # Version 2 checkpoints predate dedup: they cannot be matched against a
    # configuration that now requires window state; version 3 must be backed
    # by at least one dedup source.
    if version == CHECKPOINT_VERSION and any_dedup:
        raise CheckpointError(
            "checkpoint predates dedup_window; cannot recover window state"
        )
    if version == CHECKPOINT_VERSION_DEDUP and not any_dedup:
        raise CheckpointError(
            "checkpoint carries dedup state but the configuration sets no "
            "dedup_window"
        )
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
    # event_id is any JSON scalar (null included: null is a valid dedup key
    # that matches only null); only arrays and objects are rejected.
    if isinstance(event_id, (dict, list)):
        raise DataValidationError("%s: event_id must be a scalar" % where)
    if not isinstance(obj["payload"], dict):
        raise DataValidationError("%s: payload must be a JSON object" % where)
    return obj["source_id"], event_id, obj["payload"]


def _event_time_value(payload, parts, where):
    """Read the event time from the pre-transform payload.

    The path must exist (through nested objects) and hold a finite,
    non-boolean number; anything else is a data error.
    """
    value = _walk(payload, parts, where)
    if not is_finite_number(value):
        raise DataValidationError(
            "%s: event_time path %r must hold a finite number, got %r"
            % (where, _desc(parts), value))
    return value


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
    # The watermark check runs right after envelope validation and before
    # dedup and any transform: the event time is read from the pre-transform
    # payload and judged against the watermark as it stood before this
    # record arrived. A late record under ``drop`` is consumed (it counts
    # toward the batch and the committed prefix and slides through the
    # dedup window) but emits nothing, changes no schema version and does
    # not raise the maximum; under ``error`` it fails the batch.
    if state.event_time_parts is not None:
        event_time = _event_time_value(payload, state.event_time_parts, where)
        if state.is_late(event_time):
            if state.late_policy == "error":
                raise DataValidationError(
                    "%s: event_time %r is earlier than the watermark %r"
                    % (where, event_time, state.watermark()))
            state.note_event(event_id)
            state.records += 1
            return
        # A non-late record's event time joins the maximum even when the
        # record is later deduplicated, filtered out or exploded into zero
        # branches.
        state.note_event_time(event_time)
    # Dedup runs after the envelope is validated and before any transform:
    # a duplicate is consumed (it counts toward the batch and the committed
    # prefix and slides through the window) but is never transformed or
    # emitted and never triggers a schema change. The window gains the key
    # whether or not the record later survives a filter, so a filtered first
    # occurrence still makes every later equal key a duplicate.
    if state.note_event(event_id):
        state.records += 1
        return
    branches = apply_pipeline([payload], config.transforms, where)
    # Source-level transforms run after the shared ones, in their own
    # configured order.
    if branches:
        branches = apply_pipeline(branches, spec["transforms"], where)
    # A record whose branches all die (filtered out, or an explode over an
    # empty array) is still consumed: it counts toward the batch and the
    # committed input prefix, it just produces no output records and no
    # schema change.
    state.records += 1
    for data in branches:
        fp = schema_fingerprint(data)
        # Under ``schema_policy: compatible`` every actually emitted record
        # is judged against the source's compatibility baseline, in
        # emission order; the first incompatible output fails the batch.
        if state.schema_policy == "compatible":
            state.check_schema_compatible(fp, where)
        state.note_schema(fp)
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
