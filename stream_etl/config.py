"""Configuration loading and validation.

A configuration is a YAML mapping with two keys::

    sources:
      - id: <unique string>
        type: jsonl | csv
        path: <path to a JSON Lines file or a CSV file>
        batch_size: <positive integer>
        transforms:           # optional, source-level; run after the
          - op: ...           # shared top-level transforms
    transforms:
      - op: rename | drop | set | cast | filter
        ...

Only these keys are recognised; anything else is a ConfigurationError so a
typo cannot silently change behaviour.
"""

import hashlib
import json
import os

from . import yaml_lite
from .errors import ConfigurationError

CAST_TYPES = ("string", "integer", "number", "boolean")
FILTER_COMPARES = ("eq", "ne", "lt", "lte", "gt", "gte")
FILTER_ORDERING_COMPARES = ("lt", "lte", "gt", "gte")
SUPPORTED_SOURCE_TYPES = ("jsonl", "csv")
TRANSFORM_OPS = ("rename", "drop", "set", "cast", "filter")
TOP_LEVEL_KEYS = ("sources", "transforms")
SOURCE_REQUIRED_KEYS = ("id", "type", "path", "batch_size")
SOURCE_KEYS = SOURCE_REQUIRED_KEYS + ("transforms",)


def split_path(path):
    """Split a dotted field path into non-empty segments."""
    if not isinstance(path, str) or path == "":
        raise ConfigurationError("field path must be a non-empty string")
    if path.startswith(".") or path.endswith(".") or ".." in path:
        raise ConfigurationError("illegal field path %r" % path)
    parts = path.split(".")
    if any(part == "" or part != part.strip() for part in parts):
        raise ConfigurationError("illegal field path %r" % path)
    return parts


def _require_mapping(value, what):
    if not isinstance(value, dict):
        raise ConfigurationError("%s must be a mapping" % what)


def _is_finite_number(value):
    """A non-boolean int/float that is neither NaN nor infinite."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return value == value and value not in (float("inf"), float("-inf"))


def _validate_transform(raw, index, prefix="transforms"):
    where = "%s[%d]" % (prefix, index)
    if not isinstance(raw, dict):
        raise ConfigurationError("%s must be a mapping" % where)
    if "op" not in raw:
        raise ConfigurationError("%s is missing 'op'" % where)
    op = raw["op"]
    if not isinstance(op, str) or op not in TRANSFORM_OPS:
        raise ConfigurationError(
            "%s has unknown operation %r (expected one of %s)"
            % (where, op, ", ".join(TRANSFORM_OPS))
        )
    allowed = {"op": None}
    if op == "rename":
        allowed.update({"from": None, "to": None})
        for key in ("from", "to"):
            if key not in raw:
                raise ConfigurationError("%s (%s) requires %r" % (where, op, key))
            split_path(raw[key])
    elif op in ("drop", "cast", "set"):
        allowed["field"] = None
        if "field" not in raw:
            raise ConfigurationError("%s (%s) requires 'field'" % (where, op))
        split_path(raw["field"])
        if op == "cast":
            allowed["type"] = None
            if "type" not in raw:
                raise ConfigurationError("%s (cast) requires 'type'" % where)
            if raw["type"] not in CAST_TYPES:
                raise ConfigurationError(
                    "%s (cast) accepts only %s, got %r"
                    % (where, ", ".join(CAST_TYPES), raw["type"])
                )
        elif op == "set":
            allowed["value"] = None
            if "value" not in raw:
                raise ConfigurationError("%s (set) requires 'value'" % where)
            value = raw["value"]
            if isinstance(value, (dict, list)):
                raise ConfigurationError(
                    "%s (set) value must be a scalar" % where
                )
    elif op == "filter":
        allowed.update({"field": None, "compare": None, "value": None})
        for key in ("field", "compare", "value"):
            if key not in raw:
                raise ConfigurationError(
                    "%s (filter) requires %r" % (where, key))
        split_path(raw["field"])
        compare = raw["compare"]
        if compare not in FILTER_COMPARES:
            raise ConfigurationError(
                "%s (filter) accepts only %s, got %r"
                % (where, ", ".join(FILTER_COMPARES), compare)
            )
        value = raw["value"]
        if isinstance(value, (dict, list)):
            raise ConfigurationError(
                "%s (filter) value must be a scalar" % where
            )
        if compare in FILTER_ORDERING_COMPARES and not _is_finite_number(value):
            raise ConfigurationError(
                "%s (filter) value for %r must be a finite number"
                % (where, compare)
            )
    extra = set(raw) - set(allowed)
    if extra:
        raise ConfigurationError(
            "%s (%s) has unknown keys: %s" % (where, op, ", ".join(sorted(extra)))
        )
    # Return a plain ordered dict preserving configuration key order; the
    # fingerprint is sensitive to that order.
    return {k: raw[k] for k in raw}


def _validate_source(raw, index, seen_ids):
    where = "sources[%d]" % index
    if not isinstance(raw, dict):
        raise ConfigurationError("%s must be a mapping" % where)
    missing = [k for k in SOURCE_REQUIRED_KEYS if k not in raw]
    if missing:
        raise ConfigurationError(
            "%s is incomplete, missing: %s" % (where, ", ".join(missing))
        )
    extra = set(raw) - set(SOURCE_KEYS)
    if extra:
        raise ConfigurationError(
            "%s has unknown keys: %s" % (where, ", ".join(sorted(extra)))
        )
    sid, stype, path, batch = (raw[k] for k in SOURCE_REQUIRED_KEYS)
    if not isinstance(sid, str) or sid == "":
        raise ConfigurationError("%s.id must be a non-empty string" % where)
    if sid in seen_ids:
        raise ConfigurationError("duplicate source id %r" % sid)
    if stype not in SUPPORTED_SOURCE_TYPES:
        raise ConfigurationError(
            "%s has unsupported type %r (expected one of %s)"
            % (where, stype, ", ".join(SUPPORTED_SOURCE_TYPES))
        )
    if not isinstance(path, str) or path == "":
        raise ConfigurationError("%s.path must be a non-empty string" % where)
    if isinstance(batch, bool) or not isinstance(batch, int) or batch < 1:
        raise ConfigurationError(
            "%s.batch_size must be a positive integer" % where
        )
    raw_transforms = raw.get("transforms", [])
    if not isinstance(raw_transforms, list):
        raise ConfigurationError("%s.transforms must be a list" % where)
    prefix = "%s.transforms" % where
    transforms = [
        _validate_transform(item, i, prefix)
        for i, item in enumerate(raw_transforms)
    ]
    seen_ids.add(sid)
    return {
        "id": sid,
        "type": stype,
        "path": path,
        "batch_size": batch,
        "transforms": transforms,
    }


class Config:
    def __init__(self, sources, transforms, raw):
        self.sources = sources
        self.transforms = transforms
        self.raw = raw

    def fingerprint(self):
        """Stable hash of the configuration; embedded in the checkpoint."""
        spec = {"sources": self.sources, "transforms": self.transforms}
        blob = json.dumps(spec, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _parse_document(text):
    try:
        doc = yaml_lite.loads(text)
    except yaml_lite.YAMLError as exc:
        raise ConfigurationError(str(exc))
    if doc is None:
        raise ConfigurationError("configuration is empty")
    if not isinstance(doc, dict):
        raise ConfigurationError("top-level configuration must be a mapping")
    return doc


def build_config(doc):
    extra = set(doc) - set(TOP_LEVEL_KEYS)
    if extra:
        raise ConfigurationError(
            "unknown configuration keys: %s" % ", ".join(sorted(extra))
        )
    if "sources" not in doc:
        raise ConfigurationError("configuration is missing 'sources'")
    raw_sources = doc["sources"]
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ConfigurationError("'sources' must be a non-empty list")
    seen = set()
    sources = [
        _validate_source(item, i, seen) for i, item in enumerate(raw_sources)
    ]
    transforms = []
    raw_transforms = doc.get("transforms", [])
    if not isinstance(raw_transforms, list):
        raise ConfigurationError("'transforms' must be a list")
    for i, item in enumerate(raw_transforms):
        transforms.append(_validate_transform(item, i))
    return Config(sources, transforms, doc)


def load_config(path):
    """Read, parse and validate a configuration file."""
    try:
        with open(path, "r", encoding="utf-8") as fp:
            text = fp.read()
    except OSError as exc:
        raise ConfigurationError("cannot read %s: %s" % (path, exc))
    return build_config(_parse_document(text))


def parse_config_text(text):
    """Build a Config from YAML text (used by tests and embedding)."""
    return build_config(_parse_document(text))


def abspath(path):
    """Normalise a CLI path for checkpoint identity checks."""
    return os.path.abspath(path)
