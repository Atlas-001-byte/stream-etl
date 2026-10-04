"""End-to-end and unit tests for stream-etl."""

import io
import json
import os
import stat
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from stream_etl import engine, yaml_lite  # noqa: E402
from stream_etl.cli import main as cli_main  # noqa: E402
from stream_etl.config import load_config, parse_config_text  # noqa: E402
from stream_etl.errors import (  # noqa: E402
    CheckpointError,
    ConfigurationError,
    DataValidationError,
    SinkError,
    SourceError,
)

BIN = os.path.join(REPO_ROOT, "bin", "stream-etl")


# --------------------------------------------------------------------------
# Fixtures / helpers
# --------------------------------------------------------------------------


@pytest.fixture()
def work(tmp_path):
    return tmp_path


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path):
    with open(path, encoding="utf-8") as fp:
        return [json.loads(line) for line in fp if line.strip()]


def read_checkpoint(path):
    with open(path, encoding="utf-8") as fp:
        return json.load(fp)


CONFIG_YAML = """
sources:
  - id: users
    type: jsonl
    path: {users}
    batch_size: 2
  - id: events
    type: jsonl
    path: {events}
    batch_size: 3
transforms:
  - op: rename
    from: name
    to: full_name
  - op: cast
    field: age
    type: integer
  - op: drop
    field: secret
  - op: set
    field: source_kind
    value: test
"""


def make_config(work, yaml_text=None):
    users = work / "users.jsonl"
    events = work / "events.jsonl"
    cfg = work / "config.yaml"
    cfg.write_text(
        (yaml_text or CONFIG_YAML).format(
            users=str(users), events=str(events)
        ),
        encoding="utf-8",
    )
    return cfg, users, events


def env_record(sid, eid, payload):
    return {"source_id": sid, "event_id": eid, "payload": payload}


# --------------------------------------------------------------------------
# YAML parser
# --------------------------------------------------------------------------


def test_yaml_basic_structures():
    doc = yaml_lite.loads(
        "sources:\n"
        "  - id: a\n"
        "    batch_size: 2\n"
        "    flags:\n"
        "      x: true\n"
        "      y: false\n"
        "  - id: b\n"
        "ratio: 1.5\n"
        'note: "quoted: value"\n'
        "empty: null\n"
    )
    assert doc == {
        "sources": [
            {"id": "a", "batch_size": 2, "flags": {"x": True, "y": False}},
            {"id": "b"},
        ],
        "ratio": 1.5,
        "note": "quoted: value",
        "empty": None,
    }


@pytest.mark.parametrize(
    "text",
    [
        "a: [1, 2]",
        "a:\n\tb: 1",
        "a: 1\na: 2",
        "---\nx: 1\n---\ny: 2\n",
        "a:\n  b: 1\n b: 2",
    ],
)
def test_yaml_rejects_unsupported(text):
    with pytest.raises(yaml_lite.YAMLError):
        yaml_lite.loads(text)


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


def test_config_minimal(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n",
        encoding="utf-8",
    )
    config = load_config(str(cfg))
    assert config.sources[0]["id"] == "s"
    assert config.transforms == []


@pytest.mark.parametrize(
    "yaml_text,needle",
    [
        ("", "empty"),
        ("sources: []\n", "non-empty"),
        ("sources: nope\n", "list"),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n",
            "batch_size",
        ),
        (
            "sources:\n  - id: a\n    type: tsv\n    path: x\n"
            "    batch_size: 1\n",
            "type",
        ),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n"
            "    batch_size: 1\n  - id: a\n    type: jsonl\n    path: y\n"
            "    batch_size: 1\n",
            "duplicate",
        ),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n"
            "    batch_size: 0\n",
            "positive",
        ),
        ("sources:\n  - id: a\n    type: jsonl\n    path: x\n"
         "    batch_size: 2\nunknown: 1\n", "unknown"),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n"
            "    batch_size: 1\ntransforms:\n  - op: frobnicate\n",
            "unknown operation",
        ),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n"
            "    batch_size: 1\ntransforms:\n  - op: cast\n    field: v\n"
            "    type: float\n",
            "cast",
        ),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n"
            "    batch_size: 1\ntransforms:\n  - op: rename\n"
            "    from: a\n",
            "'to'",
        ),
        (
            "sources:\n  - id: a\n    type: jsonl\n    path: x\n"
            "    batch_size: 1\ntransforms:\n  - op: set\n    field: .bad\n"
            "    value: 1\n",
            "path",
        ),
    ],
)
def test_config_errors(yaml_text, needle):
    with pytest.raises(ConfigurationError) as exc:
        parse_config_text(yaml_text)
    assert needle in str(exc.value)


# --------------------------------------------------------------------------
# Run: happy path, ordering, output shape, checkpoints
# --------------------------------------------------------------------------


def test_run_happy_path(work):
    cfg, users, events = make_config(work)
    write_jsonl(users, [
        env_record("users", 1, {"name": "Ada", "age": "36", "secret": "x",
                                "extra": [1, 2]}),
        env_record("users", 2, {"name": "Bo", "age": 40, "secret": "y"}),
        env_record("users", 3, {"name": "Cy", "age": "29", "secret": "z"}),
    ])
    write_jsonl(events, [
        env_record("events", "e1", {"name": "n", "age": "1", "secret": "s",
                                    "meta": {"k": "v"}}),
    ])
    out = work / "out.jsonl"
    cp = work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["source_id"] for r in rows] == [
        "users", "users", "users", "events"
    ]
    assert [r["event_id"] for r in rows] == [1, 2, 3, "e1"]
    first = rows[0]
    assert set(first) == {"source_id", "event_id", "schema_version", "data"}
    assert first["data"] == {
        "full_name": "Ada", "age": 36, "extra": [1, 2],
        "source_kind": "test",
    }
    assert isinstance(first["data"]["age"], int)
    # user2 drops the "extra" field -> schema version 2; user3 same shape.
    assert rows[1]["schema_version"] == 2
    assert rows[2]["schema_version"] == 2
    # Schema versions are numbered independently per source, so the first
    # output record for "events" starts at 1 despite its nested shape.
    assert rows[3]["schema_version"] == 1
    assert rows[3]["data"]["meta"] == {"k": "v"}

    checkpoint = read_checkpoint(cp)
    assert checkpoint["version"] == 2
    assert checkpoint["sink_offset"] == out.stat().st_size
    assert checkpoint["sources"]["users"]["records"] == 3
    assert checkpoint["sources"]["events"]["records"] == 1
    assert checkpoint["sources"]["users"]["schema_version"] == 2
    assert checkpoint["sources"]["events"]["schema_version"] == 1


def test_run_writes_nothing_to_stdout(work, capsys):
    cfg, users, events = make_config(work)
    write_jsonl(users, [env_record("users", 1, {"name": "a", "age": "1",
                                                "secret": ""})])
    write_jsonl(events, [])
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o.jsonl"),
                   "-p", str(work / "c.json")])
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out == ""


def test_run_refuses_existing_output(work):
    cfg, users, events = make_config(work)
    write_jsonl(users, [])
    write_jsonl(events, [])
    out = work / "out.jsonl"
    cp = work / "cp.json"
    out.write_text("old")
    with pytest.raises(ConfigurationError):
        from stream_etl.engine import run as run_cmd
        run_cmd(load_config(str(cfg)), str(out), str(cp))


def test_run_refuses_existing_checkpoint(work):
    cfg, users, events = make_config(work)
    write_jsonl(users, [])
    write_jsonl(events, [])
    out = work / "out.jsonl"
    cp = work / "cp.json"
    cp.write_text("{}")
    from stream_etl.engine import run as run_cmd
    with pytest.raises(ConfigurationError):
        run_cmd(load_config(str(cfg)), str(out), str(cp))


# --------------------------------------------------------------------------
# Transforms
# --------------------------------------------------------------------------


def transform(data, transforms):
    return engine.apply_transforms(data, transforms, "test")


def test_rename_nested():
    data = {"a": {"b": 1}}
    out = transform(data, [{"op": "rename", "from": "a.b", "to": "a.c"}])
    assert out == {"a": {"c": 1}}


def test_rename_missing_path():
    with pytest.raises(DataValidationError):
        transform({"a": 1}, [{"op": "rename", "from": "x", "to": "y"}])


def test_rename_into_missing_parent():
    with pytest.raises(DataValidationError):
        transform({"a": 1}, [{"op": "rename", "from": "a", "to": "x.y"}])


def test_rename_refuses_overwrite():
    with pytest.raises(DataValidationError):
        transform({"a": 1, "b": 2},
                  [{"op": "rename", "from": "a", "to": "b"}])


def test_drop_nested():
    assert transform({"a": {"b": 1, "c": 2}},
                     [{"op": "drop", "field": "a.b"}]) == {"a": {"c": 2}}


def test_drop_missing():
    with pytest.raises(DataValidationError):
        transform({"a": 1}, [{"op": "drop", "field": "b"}])


def test_set_existing_and_new_leaf():
    out = transform({"a": {"b": 1}},
                    [{"op": "set", "field": "a.b", "value": 9},
                     {"op": "set", "field": "a.c", "value": "x"}])
    assert out == {"a": {"b": 9, "c": "x"}}


def test_set_missing_parent():
    with pytest.raises(DataValidationError):
        transform({}, [{"op": "set", "field": "a.b", "value": 1}])


@pytest.mark.parametrize(
    "target,value,expected",
    [
        ("string", 1, "1"),
        ("string", True, "true"),
        ("string", 1.5, "1.5"),
        ("integer", "42", 42),
        ("integer", "-7", -7),
        ("integer", 3.0, 3),
        ("number", "2.5", 2.5),
        ("number", 3, 3.0),
        ("boolean", "true", True),
        ("boolean", "false", False),
        ("boolean", 1, True),
        ("boolean", 0, False),
    ],
)
def test_cast_success(target, value, expected):
    out = transform({"v": value},
                    [{"op": "cast", "field": "v", "type": target}])
    assert out["v"] == expected
    assert out["v"] == expected  # type equality implied below
    if target == "integer":
        assert isinstance(out["v"], int) and not isinstance(out["v"], bool)
    if target == "number":
        assert isinstance(out["v"], float)
    if target == "boolean":
        assert isinstance(out["v"], bool)


@pytest.mark.parametrize(
    "target,value",
    [
        ("integer", "abc"),
        ("integer", 3.5),
        ("integer", True),
        ("integer", None),
        ("number", "nope"),
        ("number", None),
        ("boolean", "yes"),
        ("boolean", 2),
        ("boolean", None),
        ("string", None),
        ("string", {"x": 1}),
    ],
)
def test_cast_failure(target, value):
    with pytest.raises(DataValidationError):
        transform({"v": value},
                  [{"op": "cast", "field": "v", "type": target}])


def test_cast_missing_path():
    with pytest.raises(DataValidationError):
        transform({}, [{"op": "cast", "field": "v", "type": "integer"}])


def test_transforms_run_in_order():
    # rename a->b then set b: the set must see the renamed field.
    out = transform({"a": 1},
                    [{"op": "rename", "from": "a", "to": "b"},
                     {"op": "set", "field": "b", "value": 2}])
    assert out == {"b": 2}


# --------------------------------------------------------------------------
# Schema evolution
# --------------------------------------------------------------------------


SIMPLE_CFG = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 10
"""


def run_simple(work, rows, cfg_text=SIMPLE_CFG):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(cfg_text.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, rows)
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    return rc, out, cp, cfg


def test_schema_version_add_remove_change_type(work):
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"a": 1, "b": "x"}),
        env_record("s", 2, {"a": 1, "b": "x", "c": True}),       # add c
        env_record("s", 3, {"a": 1, "b": "x", "c": False}),      # same shape
        env_record("s", 4, {"a": 1, "b": "x"}),                  # c removed
        env_record("s", 5, {"a": "1", "b": "x"}),                # a type change
        env_record("s", 6, {"a": "2", "b": "different value"}),  # values only
    ])
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["schema_version"] for r in rows] == [1, 2, 2, 3, 4, 4]


def test_schema_version_nested_and_cast(work):
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 5
transforms:
  - op: cast
    field: v
    type: number
"""
    rc, out, _, _ = run_simple(work, [
        env_record("s", 1, {"v": "1", "n": {"x": 1}}),
        env_record("s", 2, {"v": "2", "n": {"x": 2, "y": 3}}),
        env_record("s", 3, {"v": "3", "n": {"x": 4}}),
        env_record("s", 4, {"v": "4", "n": {"x": 4}, "arr": [1, 2, 3]}),
        env_record("s", 5, {"v": "5", "n": {"x": 4}, "arr": [9]}),
    ], cfg_text=cfg)
    assert rc == 0
    rows = read_jsonl(out)
    # Versions only ever increase for a source; shapes seen before but not
    # adjacent still get a fresh version (records 3 and 5 revisit shapes).
    assert [r["schema_version"] for r in rows] == [1, 2, 3, 4, 4]
    assert all(isinstance(r["data"]["v"], float) for r in rows)


def test_schema_array_of_objects_and_nulls(work):
    rc, out, _, _ = run_simple(work, [
        env_record("s", 1, {"items": [{"k": 1}], "note": None}),
        env_record("s", 2, {"items": [{"k": 2}, {"k": 3, "z": 9}]}),
        env_record("s", 3, {"items": [{"k": 4}]}),
    ])
    assert rc == 0
    rows = read_jsonl(out)
    # record 2 adds a new object shape inside items -> version 2;
    # record 3 matches the shape of record 1 -> version 3 (monotone).
    assert [r["schema_version"] for r in rows] == [1, 2, 3]


# --------------------------------------------------------------------------
# Batch commit semantics & replay
# --------------------------------------------------------------------------


BATCH_CFG = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 2
"""


def test_checkpoint_advances_per_batch(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", i, {"a": i}) for i in range(5)])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [0, 1, 2, 3, 4]
    doc = read_checkpoint(cp)
    assert doc["sources"]["s"]["records"] == 5
    assert doc["sink_offset"] == out.stat().st_size


def test_replay_appends_new_rows_without_dupes(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", i, {"a": i}) for i in range(3)])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    before = read_jsonl(out)
    assert [r["event_id"] for r in before] == [0, 1, 2]

    # append two more lines to the input, then replay
    with open(src, "a", encoding="utf-8") as fp:
        for i in (3, 4):
            fp.write(json.dumps(env_record("s", i, {"a": i})) + "\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    after = read_jsonl(out)
    assert [r["event_id"] for r in after] == [0, 1, 2, 3, 4]
    assert len(after) == 5


def test_replay_with_no_new_data_is_idempotent(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", i, {"a": i}) for i in range(2)])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    size = out.stat().st_size
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert out.stat().st_size == size
    assert len(read_jsonl(out)) == 2


def test_replay_continues_schema_versioning(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {"a": 1})])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    # new data after replay: added field -> schema version keeps increasing
    with open(src, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("s", 2, {"a": 1, "b": 2})) + "\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [r["schema_version"] for r in rows] == [1, 2]
    assert read_checkpoint(cp)["sources"]["s"]["schema_version"] == 2


MULTI_CFG = """
sources:
  - id: a
    type: jsonl
    path: {a}
    batch_size: 5
  - id: b
    type: jsonl
    path: {b}
    batch_size: 5
"""


def test_replay_after_failure_in_later_source(work, capsys):
    a = work / "a.jsonl"
    b = work / "b.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(MULTI_CFG.format(a=str(a), b=str(b)), encoding="utf-8")
    write_jsonl(a, [env_record("a", i, {"k": 1}) for i in range(2)])
    # first run fails inside source b (bad payload)
    write_jsonl(b, [
        env_record("b", 1, {"k": 1}),
        {"source_id": "b", "event_id": 2},  # payload missing
    ])
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 3
    rows = read_jsonl(out)
    assert [r["source_id"] for r in rows] == ["a", "a", "b"]

    # repair source b and replay: source a must not be duplicated
    write_jsonl(b, [
        env_record("b", 1, {"k": 1}),
        env_record("b", 2, {"k": 2}),
    ])
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    rows = read_jsonl(out)
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("a", 0), ("a", 1), ("b", 1), ("b", 2),
    ]
    doc = read_checkpoint(cp)
    assert doc["sources"]["a"]["records"] == 2
    assert doc["sources"]["b"]["records"] == 2
    assert doc["sink_offset"] == out.stat().st_size


def test_replay_after_input_shrunk_is_checkpoint_error(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", i, {"a": i}) for i in range(3)])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    # input replaced by a shorter file: committed offset no longer valid
    write_jsonl(src, [env_record("s", 0, {"a": 0})])
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "cannot recover" in capsys.readouterr().err


def test_replay_truncates_uncommitted_tail(work):
    # 3 records with batch_size 2 commit after records 0 and 1; record 2 is
    # only committed at the source boundary, so first run fully commits it.
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", i, {"a": i}) for i in range(3)])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    committed_offset = read_checkpoint(cp)["sink_offset"]
    assert committed_offset == out.stat().st_size

    # Simulate a crash mid-batch: the checkpoint still points at the last
    # committed batch, while the output has a partial tail.
    with open(out, "ab") as fp:
        fp.write(b'{"source_id":"s","event_id":"ghost","schema_version":9,'
                 b'"data":{}}\n')
    assert out.stat().st_size > committed_offset
    with open(src, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("s", 99, {"a": 99})) + "\n")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [0, 1, 2, 99]
    assert read_checkpoint(cp)["sink_offset"] == out.stat().st_size


def test_failed_batch_is_not_committed(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [
        env_record("s", 0, {"a": 0}),
        env_record("s", 1, {"a": 1}),
        env_record("s", 2, {"a": "bad"}),
    ])
    cast_cfg = work / "cast.yaml"
    cast_cfg.write_text(
        BATCH_CFG.format(src=str(src))
        + "transforms:\n  - op: cast\n    field: a\n    type: integer\n",
        encoding="utf-8",
    )
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cast_cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 3
    # batch of records 0,1 committed before the failing record 2
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [0, 1]
    doc = read_checkpoint(cp)
    assert doc["sources"]["s"]["records"] == 2

    # fix the input line and replay: no duplicates, no skips
    write_jsonl(src, [
        env_record("s", 0, {"a": 0}),
        env_record("s", 1, {"a": 1}),
        env_record("s", 2, {"a": 2}),
    ])
    rc = cli_main(["replay", "-c", str(cast_cfg), "-o", str(out),
                   "-p", str(cp)])
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [0, 1, 2]


# --------------------------------------------------------------------------
# Validation errors (exit code 3)
# --------------------------------------------------------------------------


def expect_rc(args, code, capsys, needle=""):
    rc = cli_main(args)
    err = capsys.readouterr().err
    assert rc == code, err
    assert err.startswith("Error: ")
    if needle:
        assert needle in err
    return err


def test_missing_envelope_fields(work, capsys):
    rc, out, cp, cfg = run_simple(work, [
        {"source_id": "s", "event_id": 1},  # payload missing
    ])
    assert rc == 3
    err = capsys.readouterr().err
    assert err.startswith("Error: DataValidationError")
    assert "payload" in err


@pytest.mark.parametrize("row", [
    {"source_id": "s", "event_id": 1},
    {"event_id": 1, "payload": {}},
    {"source_id": "s", "payload": {}},
    {"source_id": "s", "event_id": 1, "payload": [1, 2]},
    {"source_id": "", "event_id": 1, "payload": {}},
    {"source_id": "other", "event_id": 1, "payload": {}},
])
def test_invalid_records(work, capsys, row):
    rc, out, cp, cfg = run_simple(work, [row])
    assert rc == 3


def test_malformed_json_line(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    src.write_text("{not json\n", encoding="utf-8")
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "c")])
    assert rc == 3
    assert "malformed" in capsys.readouterr().err


def test_blank_line_is_invalid(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    src.write_text("\n", encoding="utf-8")
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "c")])
    assert rc == 3


def test_json_array_line_is_invalid(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    src.write_text("[1, 2, 3]\n", encoding="utf-8")
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "c")])
    assert rc == 3
    assert "JSON object" in capsys.readouterr().err


def test_transform_path_not_found(work, capsys):
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 1
transforms:
  - op: drop
    field: nope
"""
    rc, out, cp, _ = run_simple(work, [env_record("s", 1, {"a": 1})], cfg)
    assert rc == 3
    assert "does not exist" in capsys.readouterr().err


# --------------------------------------------------------------------------
# Source / sink / checkpoint errors
# --------------------------------------------------------------------------


def test_missing_input_is_source_error(work, capsys):
    src = work / "missing.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "c")])
    assert rc == 5
    assert "SourceError" in capsys.readouterr().err


def test_unreadable_input(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    src.write_text(json.dumps(env_record("s", 1, {})) + "\n")
    os.chmod(src, 0)
    try:
        if os.geteuid() == 0:
            pytest.skip("root bypasses file permissions")
        rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                       "-p", str(work / "c")])
        assert rc == 5
        assert "SourceError" in capsys.readouterr().err
    finally:
        os.chmod(src, stat.S_IRUSR | stat.S_IWUSR)


def test_output_directory_missing_is_sink_error(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {})])
    rc = cli_main(["run", "-c", str(cfg),
                   "-o", str(work / "nodir" / "o.jsonl"),
                   "-p", str(work / "c.json")])
    assert rc == 5
    assert "SinkError" in capsys.readouterr().err


def test_replay_missing_checkpoint(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {})])
    out = work / "o.jsonl"
    out.write_text("")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out),
                   "-p", str(work / "ghost.json")])
    assert rc == 4
    assert "CheckpointError" in capsys.readouterr().err


def test_replay_missing_output(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {})])
    cp = work / "cp.json"
    # produce a real run, then delete its output
    out = work / "o.jsonl"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    os.unlink(out)
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "CheckpointError" in capsys.readouterr().err


def test_corrupt_checkpoint(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {})])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    cp.write_text("{not json")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "corrupt" in capsys.readouterr().err


def test_checkpoint_version_mismatch(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {})])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    doc = read_checkpoint(cp)
    doc["version"] = 99
    cp.write_text(json.dumps(doc))
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "version mismatch" in capsys.readouterr().err


def test_checkpoint_config_mismatch(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {})])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    cfg.write_text(SIMPLE_CFG.format(src=str(src)) +
                   "transforms:\n  - op: drop\n    field: z\n",
                   encoding="utf-8")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "different configuration" in capsys.readouterr().err


def test_truncated_output_cannot_recover(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(BATCH_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", i, {"a": i}) for i in range(4)])
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    # shrink output below the committed offset
    with open(out, "rb") as fp:
        data = fp.read()
    with open(out, "wb") as fp:
        fp.write(data[: len(data) // 4])
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "cannot recover" in capsys.readouterr().err


# --------------------------------------------------------------------------
# CLI / binary
# --------------------------------------------------------------------------


def test_unknown_command_exits_nonzero():
    with pytest.raises(SystemExit):
        cli_main(["frobnicate"])


def test_missing_required_args_exits_nonzero():
    with pytest.raises(SystemExit):
        cli_main(["run"])


def test_config_file_missing_is_configuration_error(work, capsys):
    rc = cli_main(["run", "-c", str(work / "nope.yaml"),
                   "-o", str(work / "o"), "-p", str(work / "c")])
    assert rc == 2
    assert "ConfigurationError" in capsys.readouterr().err


def test_binary_end_to_end(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SIMPLE_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {"a": 1})])
    out, cp = work / "o.jsonl", work / "cp.json"
    proc = subprocess.run(
        [BIN, "run", "-c", str(cfg), "-o", str(out), "-p", str(cp)],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == ""
    assert out.exists() and cp.exists()
    assert len(read_jsonl(out)) == 1

    # replay with no new data
    proc = subprocess.run(
        [BIN, "replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == ""
    assert len(read_jsonl(out)) == 1


def test_binary_error_prefix_and_code(work):
    proc = subprocess.run(
        [BIN, "run", "-c", str(work / "nope.yaml"),
         "-o", str(work / "o"), "-p", str(work / "c")],
        capture_output=True, text=True,
    )
    assert proc.returncode == 2
    assert proc.stderr.startswith("Error: ConfigurationError")


# --------------------------------------------------------------------------
# Source-level transforms
# --------------------------------------------------------------------------


def test_config_source_transforms_validated():
    config = parse_config_text(
        "sources:\n"
        "  - id: s\n"
        "    type: jsonl\n"
        "    path: /tmp/x\n"
        "    batch_size: 1\n"
        "    transforms:\n"
        "      - op: rename\n"
        "        from: a\n"
        "        to: b\n"
        "      - op: cast\n"
        "        field: b\n"
        "        type: integer\n"
    )
    assert config.sources[0]["transforms"] == [
        {"op": "rename", "from": "a", "to": "b"},
        {"op": "cast", "field": "b", "type": "integer"},
    ]
    # a source without transforms normalises to an empty list
    config = parse_config_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n"
    )
    assert config.sources[0]["transforms"] == []


@pytest.mark.parametrize(
    "transforms_yaml,needle",
    [
        ("    transforms: nope\n", "list"),
        ("    transforms:\n      - field: a\n", "missing 'op'"),
        ("    transforms:\n      - op: frobnicate\n", "unknown operation"),
        ("    transforms:\n      - op: drop\n        field: a\n"
         "        extra: 1\n", "unknown keys"),
        ("    transforms:\n      - op: drop\n        field: .bad\n", "path"),
        ("    transforms:\n      - op: cast\n        field: a\n"
         "        type: float\n", "cast"),
        ("    transforms:\n      - op: set\n        field: a\n"
         "        value:\n          x: 1\n", "scalar"),
    ],
)
def test_config_source_transforms_errors(transforms_yaml, needle):
    yaml_text = (
        "sources:\n"
        "  - id: s\n"
        "    type: jsonl\n"
        "    path: /tmp/x\n"
        "    batch_size: 1\n"
        + transforms_yaml
    )
    with pytest.raises(ConfigurationError) as exc:
        parse_config_text(yaml_text)
    assert needle in str(exc.value)


def test_config_source_unknown_key_still_rejected():
    with pytest.raises(ConfigurationError) as exc:
        parse_config_text(
            "sources:\n"
            "  - id: s\n"
            "    type: jsonl\n"
            "    path: /tmp/x\n"
            "    batch_size: 1\n"
            "    transform: []\n"
        )
    assert "unknown keys" in str(exc.value)


SRC_LEVEL_CFG = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 2
    transforms:
      - op: cast
        field: total
        type: number
      - op: set
        field: tag
        value: src
transforms:
  - op: rename
    from: amount
    to: total
  - op: drop
    field: secret
"""


def test_source_transforms_run_after_shared_ones(work):
    # The source-level cast sees `total`, which only exists after the shared
    # rename; the shared drop removed `secret` before source transforms run.
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"amount": "10.5", "secret": "x"}),
        env_record("s", 2, {"amount": "2", "secret": "y"}),
    ], cfg_text=SRC_LEVEL_CFG)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["data"] for r in rows] == [
        {"total": 10.5, "tag": "src"},
        {"total": 2.0, "tag": "src"},
    ]
    assert [r["schema_version"] for r in rows] == [1, 1]
    doc = read_checkpoint(cp)
    assert doc["sources"]["s"]["records"] == 2
    assert doc["sink_offset"] == out.stat().st_size


def test_source_transforms_schema_versioning(work):
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 10
    transforms:
      - op: cast
        field: v
        type: integer
"""
    rc, out, _, _ = run_simple(work, [
        env_record("s", 1, {"v": "1", "keep": "a"}),
        env_record("s", 2, {"v": "2", "keep": "a", "extra": 1}),  # add field
        env_record("s", 3, {"v": "3", "keep": "b"}),              # drop field
        env_record("s", 4, {"v": "4", "keep": "c"}),              # values only
    ], cfg_text=cfg)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["schema_version"] for r in rows] == [1, 2, 3, 3]
    assert all(isinstance(r["data"]["v"], int) for r in rows)


def test_source_transforms_error_skips_batch_commit(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SRC_LEVEL_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [
        env_record("s", 0, {"amount": "1", "secret": "a"}),
        env_record("s", 1, {"amount": "2", "secret": "b"}),
        env_record("s", 2, {"amount": "not-a-number", "secret": "c"}),
    ])
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 3
    # batch of records 0,1 committed; the failing record's batch is not
    assert [r["event_id"] for r in read_jsonl(out)] == [0, 1]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 2

    # repair the input and replay: continue without duplicates or gaps
    write_jsonl(src, [
        env_record("s", 0, {"amount": "1", "secret": "a"}),
        env_record("s", 1, {"amount": "2", "secret": "b"}),
        env_record("s", 2, {"amount": "3", "secret": "c"}),
    ])
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [0, 1, 2]
    assert rows[2]["data"] == {"total": 3.0, "tag": "src"}
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 3


MULTI_SRC_LEVEL_CFG = """
sources:
  - id: a
    type: jsonl
    path: {a}
    batch_size: 2
    transforms:
      - op: set
        field: origin
        value: alpha
  - id: b
    type: jsonl
    path: {b}
    batch_size: 2
transforms:
  - op: rename
    from: k
    to: key
"""


def test_source_transforms_multi_source_isolated(work):
    a = work / "a.jsonl"
    b = work / "b.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(MULTI_SRC_LEVEL_CFG.format(a=str(a), b=str(b)),
                   encoding="utf-8")
    write_jsonl(a, [env_record("a", i, {"k": i}) for i in range(3)])
    write_jsonl(b, [env_record("b", i, {"k": i}) for i in range(2)])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    # shared rename applies to both; the source-level set only to source a
    assert [r["data"] for r in rows] == [
        {"key": 0, "origin": "alpha"},
        {"key": 1, "origin": "alpha"},
        {"key": 2, "origin": "alpha"},
        {"key": 0},
        {"key": 1},
    ]
    assert [r["schema_version"] for r in rows] == [1, 1, 1, 1, 1]
    doc = read_checkpoint(cp)
    assert doc["sources"]["a"]["records"] == 3
    assert doc["sources"]["b"]["records"] == 2

    # append to both inputs and replay: per-source state resumes cleanly
    with open(a, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("a", 3, {"k": 3})) + "\n")
    with open(b, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("b", 2, {"k": 2, "new": 1})) + "\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("a", 0), ("a", 1), ("a", 2), ("b", 0), ("b", 1),
        ("a", 3), ("b", 2),
    ]
    assert rows[5]["data"] == {"key": 3, "origin": "alpha"}
    assert rows[6]["data"] == {"key": 2, "new": 1}
    # source b gained a field -> its schema version advances independently
    assert [r["schema_version"] for r in rows] == [1, 1, 1, 1, 1, 1, 2]


def test_source_transforms_in_config_fingerprint(work, capsys):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(SRC_LEVEL_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [env_record("s", 1, {"amount": "1", "secret": "a"})])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    # changing only the source-level transforms must invalidate the checkpoint
    cfg.write_text(
        SRC_LEVEL_CFG.format(src=str(src)).replace("value: src",
                                                   "value: other"),
        encoding="utf-8",
    )
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "different configuration" in capsys.readouterr().err


# --------------------------------------------------------------------------
# CSV sources: parsing, shape, transforms, schema versioning
# --------------------------------------------------------------------------


CSV_CFG = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
"""


def write_csv(path, text, binary=False):
    mode = "wb" if binary else "w"
    with open(path, mode) as fp:
        fp.write(text)


def run_csv(work, text, cfg_text=None, batch=10, binary=False):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    write_csv(src, text, binary=binary)
    cfg.write_text((cfg_text or CSV_CFG).format(
        src=str(src), batch=batch), encoding="utf-8")
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    return rc, out, cp, cfg


def test_csv_basic_flat_payload(work):
    rc, out, cp, _ = run_csv(
        work, "source_id,event_id,name,age\n"
              "alpha,1,Ada,36\n"
              "alpha,2,Bo,40\n")
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["1", "2"]
    assert rows[0] == {
        "source_id": "alpha", "event_id": "1", "schema_version": 1,
        "data": {"name": "Ada", "age": "36"},
    }
    # control columns never enter data
    assert set(rows[0]["data"]) == {"name", "age"}
    assert rows[1]["data"]["name"] == "Bo"


def test_csv_empty_cells_keep_empty_string(work):
    rc, out, _, _ = run_csv(
        work, "source_id,event_id,a,b\n"
              "alpha,1,,x\n")
    assert rc == 0
    assert read_jsonl(out)[0]["data"] == {"a": "", "b": "x"}


def test_csv_control_columns_out_of_order(work):
    rc, out, _, _ = run_csv(
        work, "name,event_id,age,source_id\n"
              "Ada,1,36,alpha\n")
    assert rc == 0
    rows = read_jsonl(out)
    assert rows[0]["source_id"] == "alpha"
    assert rows[0]["event_id"] == "1"
    assert rows[0]["data"] == {"name": "Ada", "age": "36"}


def test_csv_quotes_escapes_and_multiline(work):
    text = ('source_id,event_id,note\r\n'
            'alpha,1,"a,b"\r\n'
            'alpha,2,"line1\nline2"\r\n'
            'alpha,3,"she said ""hi"""\r\n'
            'alpha,4,plain\n')
    rc, out, _, _ = run_csv(work, text)
    assert rc == 0
    assert [r["data"]["note"] for r in read_jsonl(out)] == [
        "a,b", "line1\nline2", 'she said "hi"', "plain"]


def test_csv_no_trailing_newline(work):
    rc, out, _, _ = run_csv(
        work, "source_id,event_id,v\nalpha,1,a\nalpha,2,b")
    assert rc == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2"]


def test_csv_bom_accepted(work):
    text = b"\xef\xbb\xbfsource_id,event_id,v\nalpha,1,\xe4\xb8\xad\n"
    rc, out, _, _ = run_csv(work, text, binary=True)
    assert rc == 0
    rows = read_jsonl(out)
    assert rows[0]["event_id"] == "1"
    assert rows[0]["data"] == {"v": "中"}


def test_csv_shared_then_source_transforms_and_cast(work):
    cfg = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
    transforms:
      - op: cast
        field: total
        type: number
transforms:
  - op: rename
    from: amount
    to: total
  - op: drop
    field: secret
"""
    rc, out, _, _ = run_csv(
        work, "source_id,event_id,amount,secret\n"
              "alpha,1,10.5,x\n"
              "alpha,2,2,y\n", cfg_text=cfg)
    assert rc == 0
    rows = read_jsonl(out)
    assert rows[0]["data"] == {"total": 10.5}
    assert isinstance(rows[0]["data"]["total"], float)
    assert rows[1]["data"] == {"total": 2.0}


def test_csv_value_changes_do_not_bump_version(work):
    rc, out, _, _ = run_csv(
        work, "source_id,event_id,v\n"
              "alpha,1,a\n"
              "alpha,2,bb\n"
              "alpha,3,ccc\n")
    assert rc == 0
    assert [r["schema_version"] for r in read_jsonl(out)] == [1, 1, 1]


def test_csv_parser_unit_records():
    from stream_etl.engine import _CSVParser
    data = b"source_id,event_id,x\r\nalpha,1,\"a\nb\"\r\nalpha,2,z"
    fp = io.BytesIO(data)
    p = _CSVParser("s", fp, start=0, expect_header=True)
    raw_h, head, is_h = p.next_record()
    assert is_h is True and head == ["source_id", "event_id", "x"]
    assert raw_h == b"source_id,event_id,x\r\n"
    raw1, r1, is_d1 = p.next_record()
    assert is_d1 is False and r1 == ["alpha", "1", "a\nb"]
    assert raw1 == b'alpha,1,"a\nb"\r\n'
    raw2, r2, _ = p.next_record()
    assert r2 == ["alpha", "2", "z"] and raw2 == b"alpha,2,z"
    assert p.next_record() is None
    # resume exactly after the header boundary
    fp2 = io.BytesIO(data)
    fp2.seek(len(raw_h))
    p2 = _CSVParser("s", fp2, start=len(raw_h), expect_header=False)
    _raw, fields2, is_h2 = p2.next_record()
    assert is_h2 is False and fields2[1] == "1" and p2.data_index == 1


# --------------------------------------------------------------------------
# CSV validation errors (exit code 3, no half record committed)
# --------------------------------------------------------------------------


CSV_ERROR_CASES = [
    ("", "header is missing"),
    ("source_id,source_id,event_id\n", "duplicate column"),
    ("source_id,,event_id\n", "empty column name"),
    ("source_id,x\nalpha,v\n", "missing required column"),
    ("source_id,event_id,x\n\nalpha,1,v\n", "empty logical record"),
    ("source_id,event_id,x\nalpha,1\n", "expected 3 columns, got 2"),
    ("source_id,event_id,x\nalpha,1,\"oops\n", "unterminated quoted field"),
    ("source_id,event_id,x\nother,1,v\n", "does not match configured source"),
    ("source_id,event_id,x\nalpha,,v\n", "event_id must be a non-empty"),
    ("source_id,event_id,x\r", "bare carriage return"),
]


@pytest.mark.parametrize("text,needle", CSV_ERROR_CASES)
def test_csv_validation_errors(work, capsys, text, needle):
    rc, out, cp, _ = run_csv(work, text, batch=1)
    assert rc == 3
    err = capsys.readouterr().err
    assert err.startswith("Error: DataValidationError")
    assert needle in err
    # a checkpoint may only ever record the header boundary, never a record
    if cp.exists():
        assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 0


def test_csv_too_many_columns(work, capsys):
    rc, _, _, _ = run_csv(
        work, "source_id,event_id,x\nalpha,1,a,b\n", batch=1)
    assert rc == 3
    assert "expected 3 columns, got 4" in capsys.readouterr().err


def test_csv_invalid_utf8(work, capsys):
    text = b"source_id,event_id,x\nalpha,1,\xff\n"
    rc, out, cp, _ = run_csv(work, text, binary=True, batch=1)
    assert rc == 3
    assert "invalid UTF-8" in capsys.readouterr().err
    if cp.exists():
        assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 0


def test_csv_bom_only_at_start(work, capsys):
    # a BOM appearing later in the file (even at a fresh record) is rejected
    text = b"source_id,event_id,x\nalpha,1,v\n\xef\xbb\xbfalpha,2,w\n"
    rc, _, _, _ = run_csv(work, text, binary=True, batch=1)
    assert rc == 3
    assert "BOM" in capsys.readouterr().err


def test_csv_failed_batch_not_committed(work, capsys):
    cfg = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
transforms:
  - op: cast
    field: n
    type: integer
"""
    rc, out, cp, _ = run_csv(
        work, "source_id,event_id,n\n"
              "alpha,1,1\n"
              "alpha,2,2\n"
              "alpha,3,bad\n", cfg_text=cfg, batch=2)
    assert rc == 3
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2"]
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 2

    # repair and replay: no duplicates, no gaps
    write_csv(out.parent / "s.csv",
              "source_id,event_id,n\n"
              "alpha,1,1\n"
              "alpha,2,2\n"
              "alpha,3,3\n")
    rc = cli_main(["replay", "-c", str(out.parent / "c.yaml"),
                   "-o", str(out), "-p", str(cp)])
    assert rc == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2", "3"]


# --------------------------------------------------------------------------
# CSV replay: append, idempotency, truncation, prefix integrity
# --------------------------------------------------------------------------


def test_csv_replay_appends_without_dupes(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=2), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,a\nalpha,2,b\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("alpha,3,c\nalpha,4,d\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["event_id"] for r in read_jsonl(out)] == [
        "1", "2", "3", "4"]


def test_csv_replay_idempotent(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=2), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,a\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    size, cp_bytes = out.stat().st_size, cp.read_bytes()
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert out.stat().st_size == size
    assert cp.read_bytes() == cp_bytes
    assert len(read_jsonl(out)) == 1


def test_csv_replay_continues_across_multiline_record(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=1), encoding="utf-8")
    write_csv(src, 'source_id,event_id,v\nalpha,1,"x\ny"\n')
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    with open(src, "a", encoding="utf-8") as fp:
        fp.write('alpha,2,"p\nq"\n')
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["1", "2"]
    assert rows[1]["data"]["v"] == "p\nq"
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 2


def test_csv_replay_truncates_uncommitted_tail(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=1), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,a\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    committed = read_checkpoint(cp)["sink_offset"]
    with open(out, "ab") as fp:
        fp.write(b'{"source_id":"alpha","event_id":"ghost",'
                 b'"schema_version":9,"data":{}}\n')
    assert out.stat().st_size > committed
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("alpha,2,b\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2"]
    assert read_checkpoint(cp)["sink_offset"] == out.stat().st_size


def test_csv_replay_prefix_changed_is_checkpoint_error(work, capsys):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=2), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,a\nalpha,2,b\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    committed = read_checkpoint(cp)
    out_before = out.read_bytes()
    # mutate a committed byte while keeping the same length
    data = bytearray(src.read_bytes())
    data[25] = ord("Z") if data[25] != ord("Z") else ord("Y")
    src.write_bytes(bytes(data))
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "has changed" in capsys.readouterr().err
    # the committed output prefix and checkpoint are left untouched
    assert out.read_bytes() == out_before
    assert read_checkpoint(cp) == committed


def test_csv_replay_shrunk_input_is_checkpoint_error(work, capsys):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=2), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,a\nalpha,2,b\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    write_csv(src, "source_id,event_id,v\nalpha,1,a\n")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "shorter" in capsys.readouterr().err


def test_csv_replay_schema_version_continues(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=10), encoding="utf-8")
    write_csv(src, "source_id,event_id,a\nalpha,1,x\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("alpha,2,y\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["schema_version"] for r in read_jsonl(out)] == [1, 1]


# --------------------------------------------------------------------------
# CSV + jsonl mixed sources
# --------------------------------------------------------------------------


MIXED_CFG = """
sources:
  - id: j
    type: jsonl
    path: {j}
    batch_size: 1
  - id: c
    type: csv
    path: {c}
    batch_size: 1
"""


def test_mixed_jsonl_and_csv_in_config_order(work):
    j, c = work / "j.jsonl", work / "c.csv"
    cfg = work / "cfg.yaml"
    cfg.write_text(MIXED_CFG.format(j=str(j), c=str(c)), encoding="utf-8")
    write_jsonl(j, [env_record("j", 1, {"k": 1}),
                    env_record("j", 2, {"k": 2})])
    write_csv(c, "source_id,event_id,v\nc,1,a\nc,2,b\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("j", 1), ("j", 2), ("c", "1"), ("c", "2")]
    doc = read_checkpoint(cp)
    assert doc["sources"]["j"]["records"] == 2
    assert doc["sources"]["c"]["records"] == 2

    # append to both and replay: per-source boundaries resume independently
    with open(j, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("j", 3, {"k": 3})) + "\n")
    with open(c, "a", encoding="utf-8") as fp:
        fp.write("c,3,d\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("j", 1), ("j", 2), ("c", "1"), ("c", "2"), ("j", 3), ("c", "3")]


# --------------------------------------------------------------------------
# CSV configuration + I/O error mapping
# --------------------------------------------------------------------------


def test_csv_source_accepted_in_config():
    config = parse_config_text(
        "sources:\n"
        "  - id: s\n"
        "    type: csv\n"
        "    path: /tmp/x.csv\n"
        "    batch_size: 3\n")
    src = config.sources[0]
    assert src["type"] == "csv"
    assert src["id"] == "s" and src["batch_size"] == 3
    assert src["transforms"] == []
    assert set(src) == {"id", "type", "path", "batch_size", "transforms"}


def test_csv_source_no_new_config_keys():
    with pytest.raises(ConfigurationError) as exc:
        parse_config_text(
            "sources:\n"
            "  - id: s\n    type: csv\n    path: x\n"
            "    batch_size: 1\n    delimiter: ';'\n")
    assert "unknown keys" in str(exc.value)


def test_csv_missing_input_is_source_error(work, capsys):
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(work / "ghost.csv"), batch=1),
                   encoding="utf-8")
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "cp")])
    assert rc == 5
    assert "SourceError" in capsys.readouterr().err


def test_csv_binary_end_to_end(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src), batch=1), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,a\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    proc = subprocess.run(
        [BIN, "run", "-c", str(cfg), "-o", str(out), "-p", str(cp)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == ""
    assert len(read_jsonl(out)) == 1
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("alpha,2,b\n")
    proc = subprocess.run(
        [BIN, "replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2"]


# --------------------------------------------------------------------------
# Filter transforms: configuration validation
# --------------------------------------------------------------------------


def test_config_filter_valid_top_and_source_level():
    config = parse_config_text(
        "sources:\n"
        "  - id: s\n"
        "    type: jsonl\n"
        "    path: /tmp/x\n"
        "    batch_size: 1\n"
        "    transforms:\n"
        "      - op: filter\n"
        "        field: age\n"
        "        compare: gte\n"
        "        value: 18\n"
        "transforms:\n"
        "  - op: filter\n"
        "    field: meta.tag\n"
        "    compare: ne\n"
        "    value: junk\n"
    )
    assert config.transforms == [
        {"op": "filter", "field": "meta.tag", "compare": "ne",
         "value": "junk"},
    ]
    assert config.sources[0]["transforms"] == [
        {"op": "filter", "field": "age", "compare": "gte", "value": 18},
    ]


@pytest.mark.parametrize(
    "body,needle",
    [
        ("        compare: eq\n        value: 1\n", "requires 'field'"),
        ("        field: a\n        value: 1\n", "requires 'compare'"),
        ("        field: a\n        compare: eq\n", "requires 'value'"),
        ("        field: a\n        compare: equal\n        value: 1\n",
         "compare"),
        ("        field: .bad\n        compare: eq\n        value: 1\n",
         "path"),
        ("        field: a\n        compare: eq\n        value: []\n",
         "scalar"),
        ("        field: a\n        compare: eq\n        value:\n"
         "          x: 1\n", "scalar"),
        ("        field: a\n        compare: lt\n        value: abc\n",
         "finite numeric"),
        ("        field: a\n        compare: gte\n        value: true\n",
         "finite numeric"),
        ("        field: a\n        compare: lte\n        value: null\n",
         "finite numeric"),
        ("        field: a\n        compare: eq\n        value: 1\n"
         "        extra: 2\n", "unknown keys"),
    ],
)
def test_config_filter_errors(body, needle):
    yaml_text = (
        "sources:\n"
        "  - id: s\n"
        "    type: jsonl\n"
        "    path: /tmp/x\n"
        "    batch_size: 1\n"
        "    transforms:\n"
        "      - op: filter\n"
        + body
    )
    with pytest.raises(ConfigurationError) as exc:
        parse_config_text(yaml_text)
    assert needle in str(exc.value)


def test_config_filter_error_exit_code_2(work, capsys):
    src = work / "s.jsonl"
    write_jsonl(src, [env_record("s", 1, {"a": 1})])
    cfg = work / "c.yaml"
    cfg.write_text(
        "sources:\n"
        "  - id: s\n"
        "    type: jsonl\n"
        "    path: %s\n"
        "    batch_size: 1\n"
        "transforms:\n"
        "  - op: filter\n"
        "    field: a\n"
        "    compare: gt\n" % src,
        encoding="utf-8",
    )
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "cp")])
    assert rc == 2
    assert capsys.readouterr().err.startswith("Error: ConfigurationError")


# --------------------------------------------------------------------------
# Filter transforms: comparison semantics (unit level)
# --------------------------------------------------------------------------


def keep(data, transforms):
    return engine.apply_transforms(data, transforms, "test") is not None


def test_filter_eq_string():
    flt = [{"op": "filter", "field": "tag", "compare": "eq", "value": "x"}]
    assert keep({"tag": "x"}, flt)
    assert not keep({"tag": "y"}, flt)
    assert not keep({"tag": "X"}, flt)          # exact content
    assert not keep({"tag": 1}, flt)            # number != string


def test_filter_eq_null_and_bool():
    eq_null = [{"op": "filter", "field": "v", "compare": "eq", "value": None}]
    assert keep({"v": None}, eq_null)
    assert not keep({"v": 0}, eq_null)
    assert not keep({"v": False}, eq_null)
    assert not keep({"v": ""}, eq_null)
    eq_true = [{"op": "filter", "field": "v", "compare": "eq", "value": True}]
    assert keep({"v": True}, eq_true)
    assert not keep({"v": False}, eq_true)
    assert not keep({"v": 1}, eq_true)          # bool is not a number
    assert not keep({"v": "true"}, eq_true)


def test_filter_eq_numeric_across_types():
    flt = [{"op": "filter", "field": "v", "compare": "eq", "value": 1.5}]
    assert keep({"v": 1.5}, flt)
    assert keep({"v": 1.50}, flt)
    assert not keep({"v": 2}, flt)
    int_flt = [{"op": "filter", "field": "v", "compare": "eq", "value": 2}]
    assert keep({"v": 2.0}, int_flt)            # int matches float by value
    assert not keep({"v": True}, int_flt)


def test_filter_ne():
    flt = [{"op": "filter", "field": "v", "compare": "ne", "value": "x"}]
    assert not keep({"v": "x"}, flt)
    assert keep({"v": "y"}, flt)
    assert keep({"v": None}, flt)
    assert keep({"v": 1}, flt)


@pytest.mark.parametrize(
    "compare,value,kept,dropped",
    [
        ("lt", 10, [1, 9.5, -3], [10, 11]),
        ("lte", 10, [1, 10, 10.0], [10.5, 11]),
        ("gt", 10, [11, 10.5], [10, 9]),
        ("gte", 10, [10, 10.0, 11], [9.5]),
    ],
)
def test_filter_ordering(compare, value, kept, dropped):
    flt = [{"op": "filter", "field": "v", "compare": compare,
            "value": value}]
    for number in kept:
        assert keep({"v": number}, flt)
    for number in dropped:
        assert not keep({"v": number}, flt)


def test_filter_nested_path():
    flt = [{"op": "filter", "field": "a.b.c", "compare": "eq", "value": 1}]
    assert keep({"a": {"b": {"c": 1}}}, flt)
    assert not keep({"a": {"b": {"c": 2}}}, flt)


@pytest.mark.parametrize(
    "data,compare",
    [
        ({}, "eq"),                             # field missing
        ({"a": 1}, "eq"),                       # field missing
        ({"a": {"b": 1}}, "eq"),                # a.b missing below a
        ({"v": [1, 2]}, "eq"),                  # array field
        ({"v": {"x": 1}}, "eq"),                # object field
        ({"v": "str"}, "lt"),                   # ordering on string
        ({"v": True}, "gte"),                   # ordering on bool
        ({"v": None}, "gt"),                    # ordering on null
        ({"v": float("inf")}, "eq"),            # non-finite number
        ({"v": float("nan")}, "ne"),            # non-finite number
        ({"v": float("-inf")}, "lt"),           # non-finite number
    ],
)
def test_filter_data_validation_errors(data, compare):
    flt = [{"op": "filter", "field": "v", "compare": compare, "value": 1}]
    with pytest.raises(DataValidationError):
        engine.apply_transforms(data, flt, "test")


def test_filter_parent_not_object():
    flt = [{"op": "filter", "field": "a.b", "compare": "eq", "value": 1}]
    with pytest.raises(DataValidationError):
        engine.apply_transforms({"a": 5}, flt, "test")


def test_filter_acts_in_sequence_with_other_ops():
    # set runs before the filter and the filter sees the new field
    out = engine.apply_transforms(
        {"v": 5},
        [{"op": "set", "field": "kind", "value": "big"},
         {"op": "filter", "field": "kind", "compare": "eq", "value": "big"}],
        "test")
    assert out == {"v": 5, "kind": "big"}
    # a record dropped by an early filter never reaches later transforms
    assert engine.apply_transforms(
        {"v": 1},
        [{"op": "filter", "field": "v", "compare": "gt", "value": 100},
         {"op": "drop", "field": "v"}],
        "test") is None


# --------------------------------------------------------------------------
# Filter transforms: end to end (jsonl)
# --------------------------------------------------------------------------


FILTER_CFG = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 2
transforms:
  - op: filter
    field: keep
    compare: eq
    value: "yes"
"""


def test_filter_run_drops_matching_records(work):
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"keep": "yes", "v": 1}),
        env_record("s", 2, {"keep": "no", "v": 2}),
        env_record("s", 3, {"keep": "yes", "v": 3}),
        env_record("s", 4, {"keep": "no", "v": 4}),
        env_record("s", 5, {"keep": "yes", "v": 5}),
    ], cfg_text=FILTER_CFG)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [1, 3, 5]
    assert rows[0] == {"source_id": "s", "event_id": 1, "schema_version": 1,
                       "data": {"keep": "yes", "v": 1}}
    # filtered records still count as consumed input records
    doc = read_checkpoint(cp)
    assert doc["sources"]["s"]["records"] == 5
    assert doc["sink_offset"] == out.stat().st_size


def test_filter_all_records_filtered(work):
    rc, out, cp, _ = run_simple(work, [
        env_record("s", i, {"keep": "no"}) for i in range(3)
    ], cfg_text=FILTER_CFG)
    assert rc == 0
    assert read_jsonl(out) == []
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 3


def test_filtered_records_count_toward_batch(work):
    # batch_size 2: record 0 is filtered, record 1 survives -> the batch
    # boundary commits after two *input* records; record 2 then fails, so
    # exactly the first batch is visible in the checkpoint.
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 2
transforms:
  - op: filter
    field: keep
    compare: eq
    value: "yes"
  - op: cast
    field: n
    type: integer
"""
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 0, {"keep": "no", "n": "0"}),    # filtered
        env_record("s", 1, {"keep": "yes", "n": "1"}),   # survives, commit
        env_record("s", 2, {"keep": "yes", "n": "bad"}),  # cast fails
    ], cfg_text=cfg)
    assert rc == 3
    assert [r["event_id"] for r in read_jsonl(out)] == [1]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 2


def test_filter_does_not_create_schema_versions(work):
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 10
transforms:
  - op: filter
    field: keep
    compare: eq
    value: "yes"
"""
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"keep": "yes", "a": 1}),
        # filtered record has a different shape; it must not bump the version
        env_record("s", 2, {"keep": "no", "a": 1, "extra": True}),
        env_record("s", 3, {"keep": "yes", "a": 2}),
        # surviving record with a new shape bumps the version as usual
        env_record("s", 4, {"keep": "yes", "a": 3, "extra": False}),
    ], cfg_text=cfg)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [1, 3, 4]
    assert [r["schema_version"] for r in rows] == [1, 1, 2]
    assert read_checkpoint(cp)["sources"]["s"]["schema_version"] == 2


def test_filter_shared_then_source_level(work):
    # the shared set creates the field the source-level filter checks
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 5
    transforms:
      - op: filter
        field: kind
        compare: ne
        value: junk
transforms:
  - op: set
    field: kind
    value: junk
"""
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"v": 1}),
        env_record("s", 2, {"v": 2}),
    ], cfg_text=cfg)
    assert rc == 0
    assert read_jsonl(out) == []
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 2


def test_filter_error_aborts_run_with_exit_3(work, capsys):
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"keep": "yes"}),
        env_record("s", 2, {"other": 1}),       # filter field missing
    ], cfg_text=FILTER_CFG)
    assert rc == 3
    err = capsys.readouterr().err
    assert err.startswith("Error: DataValidationError")
    # batch_size 2: nothing committed, but the surviving record was written
    # to the (uncommitted) output tail
    assert not cp.exists() or read_checkpoint(cp)["sources"]["s"][
        "records"] == 0


def test_filter_replay_no_dupes_no_skips(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(FILTER_CFG.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [
        env_record("s", 1, {"keep": "yes"}),
        env_record("s", 2, {"keep": "no"}),
    ])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["event_id"] for r in read_jsonl(out)] == [1]

    with open(src, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("s", 3, {"keep": "no"})) + "\n")
        fp.write(json.dumps(env_record("s", 4, {"keep": "yes"})) + "\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [1, 4]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 4

    # replaying again is a no-op
    size = out.stat().st_size
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert out.stat().st_size == size
    assert [r["event_id"] for r in read_jsonl(out)] == [1, 4]


def test_filter_replay_after_failure_resumes_filtered_prefix(work):
    # first run dies on record 2; after repair the replay must not re-emit
    # record 1 nor skip the filtered record 0's successors
    cfg = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: 2
transforms:
  - op: filter
    field: keep
    compare: eq
    value: "yes"
  - op: cast
    field: n
    type: integer
"""
    src = work / "s.jsonl"
    cfg_path = work / "c.yaml"
    cfg_path.write_text(cfg.format(src=str(src)), encoding="utf-8")
    write_jsonl(src, [
        env_record("s", 0, {"keep": "no", "n": "0"}),
        env_record("s", 1, {"keep": "yes", "n": "1"}),
        env_record("s", 2, {"keep": "yes", "n": "bad"}),
    ])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg_path), "-o", str(out),
                     "-p", str(cp)]) == 3
    write_jsonl(src, [
        env_record("s", 0, {"keep": "no", "n": "0"}),
        env_record("s", 1, {"keep": "yes", "n": "1"}),
        env_record("s", 2, {"keep": "yes", "n": "2"}),
    ])
    assert cli_main(["replay", "-c", str(cfg_path), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [1, 2]
    assert [r["data"]["n"] for r in rows] == [1, 2]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 3


# --------------------------------------------------------------------------
# Filter transforms: CSV sources
# --------------------------------------------------------------------------


def test_csv_filter_on_string_field(work):
    cfg = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
transforms:
  - op: filter
    field: kind
    compare: ne
    value: skip
"""
    rc, out, cp, _ = run_csv(
        work, "source_id,event_id,kind,v\n"
              "alpha,1,keep,a\n"
              "alpha,2,skip,b\n"
              "alpha,3,keep,c\n", cfg_text=cfg, batch=2)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["1", "3"]
    assert rows[0]["data"] == {"kind": "keep", "v": "a"}
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 3


def test_csv_filter_after_source_level_cast(work):
    cfg = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
    transforms:
      - op: cast
        field: age
        type: integer
      - op: filter
        field: age
        compare: gte
        value: 18
"""
    rc, out, cp, _ = run_csv(
        work, "source_id,event_id,age\n"
              "alpha,1,36\n"
              "alpha,2,7\n"
              "alpha,3,18\n", cfg_text=cfg, batch=5)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["1", "3"]
    assert [r["data"]["age"] for r in rows] == [36, 18]
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 3


def test_csv_filter_replay_idempotent(work):
    cfg = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
transforms:
  - op: filter
    field: v
    compare: eq
    value: keep
"""
    src = work / "s.csv"
    cfg_path = work / "c.yaml"
    cfg_path.write_text(cfg.format(src=str(src), batch=2), encoding="utf-8")
    write_csv(src, "source_id,event_id,v\nalpha,1,keep\nalpha,2,drop\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg_path), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1"]
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("alpha,3,keep\n")
    assert cli_main(["replay", "-c", str(cfg_path), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "3"]
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 3


# --------------------------------------------------------------------------
# Bounded dedup (dedup_window): configuration
# --------------------------------------------------------------------------


def test_config_dedup_window_accepted():
    config = parse_config_text(
        "sources:\n"
        "  - id: s\n"
        "    type: jsonl\n"
        "    path: /tmp/x\n"
        "    batch_size: 1\n"
        "    dedup_window: 5\n"
    )
    src = config.sources[0]
    assert src["dedup_window"] == 5
    assert set(src) == {
        "id", "type", "path", "batch_size", "dedup_window", "transforms"}


def test_config_without_dedup_window_has_no_key():
    # The key is absent (not normalised to None) so the fingerprint stays
    # byte-identical to the pre-dedup configuration layout.
    config = parse_config_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n"
    )
    assert "dedup_window" not in config.sources[0]


def test_config_dedup_window_one_accepted():
    config = parse_config_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n    dedup_window: 1\n"
    )
    assert config.sources[0]["dedup_window"] == 1


@pytest.mark.parametrize("value", ["true", "0", "-3", "1.5", '"3"', "null"])
def test_config_dedup_window_invalid(value):
    yaml_text = (
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n    dedup_window: %s\n" % value
    )
    with pytest.raises(ConfigurationError) as exc:
        parse_config_text(yaml_text)
    assert "dedup_window" in str(exc.value)


def test_config_dedup_window_enters_fingerprint():
    without = parse_config_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n"
    )
    with_window = parse_config_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n    dedup_window: 4\n"
    )
    assert without.fingerprint() != with_window.fingerprint()
    # changing the window size also changes the fingerprint
    other = parse_config_text(
        "sources:\n  - id: s\n    type: jsonl\n    path: /tmp/x\n"
        "    batch_size: 1\n    dedup_window: 5\n"
    )
    assert with_window.fingerprint() != other.fingerprint()


# --------------------------------------------------------------------------
# Bounded dedup: sliding-window unit semantics
# --------------------------------------------------------------------------


def _dedup_state(window):
    from stream_etl.engine import SourceState
    return SourceState(
        {"id": "s", "path": "/tmp/x", "transforms": [],
         "dedup_window": window})


def test_window_first_occurrence_then_duplicate():
    st = _dedup_state(3)
    assert st.note_event(1) is False
    assert st.note_event(1) is True
    assert list(st.window) == [1, 1]


def test_window_numeric_cross_type_equality():
    st = _dedup_state(4)
    assert st.note_event(1) is False
    assert st.note_event(1.0) is True        # int equals float by value
    # the numeric-equivalent key is still in the window after the oldest 1
    # slid out (capacity N-1 == 3): window is [1, 1.0, True] then look up 1
    assert st.note_event(True) is False      # bool never equals a number
    assert st.note_event(1) is True


def test_window_bool_and_null_distinct():
    st = _dedup_state(5)
    assert st.note_event(True) is False
    assert st.note_event(True) is True
    assert st.note_event(1) is False
    assert st.note_event(None) is False
    assert st.note_event(None) is True
    assert st.note_event(0) is False
    assert st.note_event(False) is False
    assert st.note_event("null") is False


def test_window_string_exact_content():
    st = _dedup_state(3)
    assert st.note_event("x") is False
    assert st.note_event("x") is True
    assert st.note_event("X") is False


def test_window_slides_old_keys_out():
    st = _dedup_state(3)                   # capacity 2
    assert st.note_event("a") is False
    assert st.note_event("b") is False
    assert st.note_event("a") is True       # a in [a, b]
    assert st.note_event("c") is False      # window now [b, a]; add c -> [a, c]
    assert st.note_event("b") is False      # b slid out: first occurrence again


def test_window_size_one_never_dedups():
    st = _dedup_state(1)
    assert st.note_event("a") is False
    assert st.note_event("a") is False
    assert list(st.window) == []


def test_window_disabled_state():
    from stream_etl.engine import SourceState
    st = SourceState({"id": "s", "path": "/tmp/x", "transforms": []})
    assert st.window is None
    assert st.note_event("a") is False
    assert st.note_event("a") is False


# --------------------------------------------------------------------------
# Bounded dedup: jsonl end to end
# --------------------------------------------------------------------------


DEDUP_CFG = """
sources:
  - id: s
    type: jsonl
    path: {src}
    batch_size: {batch}
    dedup_window: {window}
"""


def run_dedup(work, rows, window=10, batch=10, cfg_text=None, extra=""):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    template = cfg_text or DEDUP_CFG
    cfg.write_text(
        template.format(src=str(src), batch=batch, window=window) + extra,
        encoding="utf-8")
    write_jsonl(src, rows)
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    return rc, out, cp, cfg, src


def test_dedup_jsonl_basic_scalar_semantics(work):
    rc, out, cp, _, _ = run_dedup(work, [
        env_record("s", 1, {"v": "a"}),
        env_record("s", 1.0, {"v": "b"}),       # numeric duplicate
        env_record("s", "1", {"v": "c"}),       # string differs
        env_record("s", True, {"v": "d"}),      # bool differs from number
        env_record("s", None, {"v": "e"}),
        env_record("s", None, {"v": "f"}),      # null duplicate
    ], window=10)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [1, "1", True, None]
    doc = read_checkpoint(cp)
    assert doc["version"] == 3
    assert doc["sources"]["s"]["records"] == 6
    assert doc["sources"]["s"]["dedup_window"] == 10
    assert doc["sink_offset"] == out.stat().st_size


def test_dedup_window_boundary_slide(work):
    # window 3 -> remember only the last 2 consumed keys
    rc, out, _, _, _ = run_dedup(work, [
        env_record("s", 1, {"v": 0}),
        env_record("s", 2, {"v": 1}),
        env_record("s", 1, {"v": 2}),            # dup
        env_record("s", 3, {"v": 3}),
        env_record("s", 1, {"v": 4}),            # 1 still in [1,3] -> dup
        env_record("s", 2, {"v": 5}),            # 2 slid out -> kept
    ], window=3)
    assert rc == 0
    assert [r["data"]["v"] for r in read_jsonl(out)] == [0, 1, 3, 5]


def test_dedup_size_one_emits_everything(work):
    rc, out, cp, _, _ = run_dedup(work, [
        env_record("s", 1, {"v": 1}),
        env_record("s", 1, {"v": 2}),
        env_record("s", 1, {"v": 3}),
    ], window=1)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["data"]["v"] for r in rows] == [1, 2, 3]
    assert read_checkpoint(cp)["sources"]["s"]["dedup_keys"] == []


def test_dedup_duplicates_count_toward_batch(work):
    # batch_size 2; duplicates still advance the committed record boundary.
    rc, out, cp, _, _ = run_dedup(work, [
        env_record("s", 1, {"v": 0}),
        env_record("s", 1, {"v": 1}),            # dup -> commit after it
        env_record("s", 2, {"v": 2}),
        env_record("s", 2, {"v": 3}),            # dup -> commit after it
        env_record("s", 3, {"v": 4}),
    ], window=5, batch=2)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == [1, 2, 3]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 5


def test_dedup_duplicate_does_not_change_schema(work):
    # A duplicate carrying a wildly different payload shape must not bump
    # schema_version, nor must a different-shaped duplicate prime a shape.
    rc, out, cp, _, _ = run_dedup(work, [
        env_record("s", 1, {"a": 1}),
        env_record("s", 1, {"a": 1, "b": 2, "c": [{"x": 1}]}),  # dup
        env_record("s", 2, {"a": 1}),
    ], window=5)
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["schema_version"] for r in rows] == [1, 1]
    assert read_checkpoint(cp)["sources"]["s"]["schema_version"] == 1


def test_dedup_runs_before_transforms(work):
    # The duplicate's payload would fail the cast if it were transformed;
    # dedup must short-circuit before any transform runs.
    rc, out, _, _, _ = run_dedup(work, [
        env_record("s", 1, {"n": "1"}),
        env_record("s", 1, {"n": "not-a-number"}),  # dup, cast never runs
        env_record("s", 2, {"n": "2"}),
    ], window=5, extra="transforms:\n  - op: cast\n    field: n\n    type: integer\n")
    assert rc == 0
    assert [r["event_id"] for r in read_jsonl(out)] == [1, 2]


def test_dedup_filtered_first_occurrence_still_marks_key(work):
    rc, out, cp, _, _ = run_dedup(work, [
        env_record("s", "x", {"keep": "no"}),     # filtered, but consumed
        env_record("s", "x", {"keep": "yes"}),    # still a duplicate
        env_record("s", "y", {"keep": "yes"}),
    ], window=10, extra=(
        "transforms:\n  - op: filter\n    field: keep\n"
        '    compare: eq\n    value: "yes"\n'))
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["y"]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 3


def test_dedup_event_id_null_is_valid_scalar(work):
    rc, out, _, _, _ = run_dedup(work, [
        env_record("s", None, {"v": 1}),
        env_record("s", None, {"v": 2}),
    ], window=3)
    assert rc == 0
    assert [r["event_id"] for r in read_jsonl(out)] == [None]


@pytest.mark.parametrize("eid", [[1, 2], {"x": 1}])
def test_dedup_event_id_array_or_object_is_data_error(work, capsys, eid):
    rc, _, _, _, _ = run_dedup(work, [
        {"source_id": "s", "event_id": eid, "payload": {"v": 1}},
    ], window=3)
    assert rc == 3
    assert "event_id must be a scalar" in capsys.readouterr().err


# --------------------------------------------------------------------------
# Bounded dedup: replay equivalence, batching, idempotence
# --------------------------------------------------------------------------


DEDUP_ROWS = [
    env_record("s", 1, {"v": 0}),
    env_record("s", 2, {"v": 1}),
    env_record("s", 1, {"v": 2}),
    env_record("s", 3, {"v": 3}),
    env_record("s", 2, {"v": 4}),
    env_record("s", 4, {"v": 5}),
    env_record("s", 1, {"v": 6}),
]


def test_dedup_replay_segmented_matches_continuous(work):
    # Continuous reference.
    rc, cont_out, cont_cp, _, _ = run_dedup(
        work, DEDUP_ROWS, window=4, batch=2)
    assert rc == 0
    cont_bytes = cont_out.read_bytes()
    cont_keys = read_checkpoint(cont_cp)["sources"]["s"]["dedup_keys"]

    # Segmented: first 3 records committed, then append and replay.
    seg = work / "seg"
    seg.mkdir()
    seg_src = seg / "s.jsonl"
    seg_cfg = seg / "c.yaml"
    seg_cfg.write_text(
        DEDUP_CFG.format(src=str(seg_src), batch=2, window=4),
        encoding="utf-8")
    write_jsonl(seg_src, DEDUP_ROWS[:3])
    seg_out, seg_cp = seg / "o.jsonl", seg / "cp.json"
    assert cli_main(["run", "-c", str(seg_cfg), "-o", str(seg_out),
                     "-p", str(seg_cp)]) == 0
    with open(seg_src, "a", encoding="utf-8") as fp:
        for row in DEDUP_ROWS[3:]:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    assert cli_main(["replay", "-c", str(seg_cfg), "-o", str(seg_out),
                     "-p", str(seg_cp)]) == 0
    assert seg_out.read_bytes() == cont_bytes
    assert (read_checkpoint(seg_cp)["sources"]["s"]["dedup_keys"]
            == cont_keys)


def test_dedup_replay_idempotent_without_new_input(work):
    rc, out, cp, _, _ = run_dedup(work, DEDUP_ROWS, window=4, batch=2)
    assert rc == 0
    size, cp_bytes = out.stat().st_size, cp.read_bytes()
    assert cli_main(["replay", "-c", str(work / "c.yaml"), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert out.stat().st_size == size
    assert cp.read_bytes() == cp_bytes


def test_dedup_window_restored_after_failed_batch(work):
    # batch_size 2; the third record is a duplicate and the fourth fails a
    # cast, so the batch spanning them never commits. On replay the window
    # must be restored to the committed state and the duplicate re-decided.
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(
        DEDUP_CFG.format(src=str(src), batch=2, window=5)
        + "transforms:\n  - op: cast\n    field: n\n    type: integer\n",
        encoding="utf-8")
    write_jsonl(src, [
        env_record("s", 1, {"n": "1"}),
        env_record("s", 2, {"n": "2"}),
        env_record("s", 1, {"n": "3"}),          # duplicate
        env_record("s", 3, {"n": "bad"}),        # cast failure
    ])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 3
    assert [r["event_id"] for r in read_jsonl(out)] == [1, 2]
    assert read_checkpoint(cp)["sources"]["s"]["records"] == 2

    # repair and replay: record 3 stays a duplicate against the restored
    # window; record 3 (key 3) is emitted once.
    write_jsonl(src, [
        env_record("s", 1, {"n": "1"}),
        env_record("s", 2, {"n": "2"}),
        env_record("s", 1, {"n": "3"}),
        env_record("s", 3, {"n": "4"}),
    ])
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [(r["event_id"], r["data"]["n"]) for r in rows] == [
        (1, 1), (2, 2), (3, 4)]
    doc = read_checkpoint(cp)
    assert doc["sources"]["s"]["records"] == 4
    # the duplicate (second key 1) slid through the window like every
    # consumed record, so it is part of the restored keys
    assert doc["sources"]["s"]["dedup_keys"] == [1, 2, 1, 3]


def test_dedup_replay_appends_after_window_slid(work):
    src = work / "s.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(
        DEDUP_CFG.format(src=str(src), batch=10, window=3), encoding="utf-8")
    write_jsonl(src, [
        env_record("s", 1, {"v": 0}),
        env_record("s", 2, {"v": 1}),
        env_record("s", 3, {"v": 2}),
    ])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    # committed window is [2, 3]; key 1 has slid out so it is a first
    # occurrence again, while key 3 is still windowed and stays a duplicate
    with open(src, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("s", 1, {"v": 3})) + "\n")
        fp.write(json.dumps(env_record("s", 3, {"v": 4})) + "\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["data"]["v"] for r in read_jsonl(out)] == [0, 1, 2, 3]
    doc = read_checkpoint(cp)
    assert doc["sources"]["s"]["records"] == 5
    assert doc["sources"]["s"]["dedup_keys"] == [1, 3]


# --------------------------------------------------------------------------
# Bounded dedup: per-source isolation and checkpoint compatibility
# --------------------------------------------------------------------------


def test_dedup_windows_are_isolated_between_sources(work):
    a, b = work / "a.jsonl", work / "b.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(
        "sources:\n"
        "  - id: a\n    type: jsonl\n    path: %s\n    batch_size: 1\n"
        "    dedup_window: 5\n"
        "  - id: b\n    type: jsonl\n    path: %s\n    batch_size: 1\n"
        % (a, b), encoding="utf-8")
    write_jsonl(a, [env_record("a", 1, {}), env_record("a", 1, {})])
    write_jsonl(b, [env_record("b", 1, {}), env_record("b", 1, {})])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    # source a dedups; source b (no window) keeps both
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("a", 1), ("b", 1), ("b", 1)]
    doc = read_checkpoint(cp)
    assert doc["version"] == 3
    assert "dedup_keys" in doc["sources"]["a"]
    assert "dedup_keys" not in doc["sources"]["b"]


def test_checkpoint_without_dedup_stays_version_2(work):
    rc, out, cp, _ = run_simple(work, [
        env_record("s", 1, {"a": 1}),
        env_record("s", 1, {"a": 2}),
    ])
    assert rc == 0
    doc = read_checkpoint(cp)
    assert doc["version"] == 2
    assert "dedup_keys" not in doc["sources"]["s"]
    assert "dedup_window" not in doc["sources"]["s"]


def _mutate_checkpoint(cp, mutator):
    doc = read_checkpoint(cp)
    mutator(doc)
    cp.write_text(json.dumps(doc), encoding="utf-8")


def test_dedup_replay_checkpoint_error_cases(work, capsys):
    rc, out, cp, cfg, src = run_dedup(work, [
        env_record("s", 1, {"v": 1}),
        env_record("s", 2, {"v": 2}),
    ], window=3, batch=1)
    assert rc == 0
    with open(src, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("s", 3, {"v": 3})) + "\n")

    def expect_error(mutator, needle):
        backup = cp.read_bytes()
        _mutate_checkpoint(cp, mutator)
        r = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
        assert r == 4
        assert needle in capsys.readouterr().err
        cp.write_bytes(backup)

    expect_error(lambda d: d["sources"]["s"].pop("dedup_keys"),
                 "missing the dedup window")
    expect_error(lambda d: d["sources"]["s"].pop("dedup_window"),
                 "missing the dedup window")
    expect_error(lambda d: d["sources"]["s"].__setitem__("dedup_window", 9),
                 "configuration is 3")
    expect_error(lambda d: d["sources"]["s"].__setitem__(
        "dedup_keys", [1, 2, 3]), "exceeds its configured window")
    expect_error(lambda d: d["sources"]["s"].__setitem__(
        "dedup_keys", [9]), "inconsistent with its record count")
    expect_error(lambda d: d["sources"]["s"].__setitem__(
        "dedup_keys", [[1], 2]), "non-scalar event_id")
    expect_error(lambda d: d["sources"]["s"].__setitem__(
        "dedup_keys", 5), "window for source 's' is invalid")
    expect_error(lambda d: d.__setitem__("version", 2),
                 "predates dedup_window")


def test_dedup_old_version2_checkpoint_rejected_with_dedup_config(
        work, capsys):
    rc, out, cp, cfg, src = run_dedup(work, [
        env_record("s", 1, {"v": 1}),
    ], window=3, batch=1)
    assert rc == 0
    _mutate_checkpoint(cp, lambda d: d.__setitem__("version", 2))
    r = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert r == 4
    assert "predates dedup_window" in capsys.readouterr().err


def test_dedup_window_change_invalidates_checkpoint(work, capsys):
    rc, out, cp, cfg, src = run_dedup(work, [
        env_record("s", 1, {"v": 1}),
    ], window=3, batch=1)
    assert rc == 0
    cfg.write_text(
        DEDUP_CFG.format(src=str(src), batch=1, window=4), encoding="utf-8")
    r = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert r == 4
    assert "different configuration" in capsys.readouterr().err


# --------------------------------------------------------------------------
# Bounded dedup: CSV sources
# --------------------------------------------------------------------------


CSV_DEDUP_CFG = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
    dedup_window: {window}
"""


def run_csv_dedup(work, text, window=3, batch=10, cfg_text=None):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    write_csv(src, text)
    cfg.write_text((cfg_text or CSV_DEDUP_CFG).format(
        src=str(src), batch=batch, window=window), encoding="utf-8")
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    return rc, out, cp, cfg, src


def test_csv_dedup_basic(work):
    rc, out, cp, _, _ = run_csv_dedup(
        work, "source_id,event_id,v\n"
              "alpha,1,a\n"
              "alpha,1,b\n"
              "alpha,2,c\n"
              "alpha,1,d\n")
    assert rc == 0
    rows = read_jsonl(out)
    assert [(r["event_id"], r["data"]["v"]) for r in rows] == [
        ("1", "a"), ("2", "c")]
    doc = read_checkpoint(cp)
    assert doc["version"] == 3
    assert doc["sources"]["alpha"]["records"] == 4
    assert doc["sources"]["alpha"]["dedup_keys"] == ["2", "1"]


def test_csv_dedup_parity_with_jsonl_slide(work):
    rc, out, _, _, _ = run_csv_dedup(
        work, "source_id,event_id,v\n"
              "alpha,a,0\n"
              "alpha,b,1\n"
              "alpha,a,2\n"
              "alpha,c,3\n"
              "alpha,b,4\n", window=3)
    assert rc == 0
    # a repeats inside the window (dropped); b slides out and is kept again
    assert [r["data"]["v"] for r in read_jsonl(out)] == ["0", "1", "3", "4"]


def test_csv_dedup_replay(work):
    src = work / "s.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_DEDUP_CFG.format(src=str(src), batch=2, window=10),
                   encoding="utf-8")
    write_csv(src, "source_id,event_id,v\n"
                   "alpha,1,a\n"
                   "alpha,1,b\n"
                   "alpha,2,c\n")
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)]) == 0
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("alpha,2,d\nalpha,3,e\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2", "3"]
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 5


def test_csv_dedup_with_filter(work):
    cfg = """
sources:
  - id: alpha
    type: csv
    path: {src}
    batch_size: {batch}
    dedup_window: {window}
transforms:
  - op: filter
    field: keep
    compare: eq
    value: "yes"
"""
    rc, out, cp, _, _ = run_csv_dedup(
        work, "source_id,event_id,keep,v\n"
              "alpha,1,no,a\n"
              "alpha,1,yes,b\n"
              "alpha,2,yes,c\n", window=5, cfg_text=cfg)
    assert rc == 0
    # first 1 is filtered but still consumed, so the second 1 is a duplicate
    assert [r["event_id"] for r in read_jsonl(out)] == ["2"]
    assert read_checkpoint(cp)["sources"]["alpha"]["records"] == 3

