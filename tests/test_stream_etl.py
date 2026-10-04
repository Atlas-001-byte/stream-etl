"""End-to-end and unit tests for stream-etl."""

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
    assert checkpoint["version"] == 1
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
# CSV sources
# --------------------------------------------------------------------------


CSV_CFG = """
sources:
  - id: c
    type: csv
    path: {src}
    batch_size: 2
"""


def write_csv(path, text):
    with open(path, "wb") as fp:
        fp.write(text.encode("utf-8"))


def run_csv(work, csv_text, cfg_text=CSV_CFG, extra_cfg=""):
    src = work / "c.csv"
    cfg = work / "c.yaml"
    cfg.write_text(cfg_text.format(src=str(src)) + extra_cfg, encoding="utf-8")
    write_csv(src, csv_text)
    out, cp = work / "o.jsonl", work / "cp.json"
    rc = cli_main(["run", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    return rc, out, cp, cfg, src


def test_config_csv_type_accepted():
    config = parse_config_text(
        "sources:\n  - id: s\n    type: csv\n    path: /tmp/x.csv\n"
        "    batch_size: 1\n"
    )
    assert config.sources[0]["type"] == "csv"
    assert config.sources[0]["transforms"] == []


def test_csv_happy_path_bom_crlf_quoting(work):
    rc, out, cp, _, _ = run_csv(
        work,
        "﻿source_id,event_id,name,note\r\n"
        "c,e1,Alice,\"hello, world\"\n"
        "c,e2,\"Bob \"\"B\"\"\",\"line1\nline2\"\r\n"
        "c,e3,,\n",
    )
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["e1", "e2", "e3"]
    assert rows[0]["data"] == {"name": "Alice", "note": "hello, world"}
    assert rows[1]["data"] == {"name": 'Bob "B"', "note": "line1\nline2"}
    # empty cells stay empty strings; control columns never enter data
    assert rows[2]["data"] == {"name": "", "note": ""}
    assert [r["schema_version"] for r in rows] == [1, 1, 1]
    doc = read_checkpoint(cp)
    assert doc["sources"]["c"]["records"] == 3
    assert doc["sink_offset"] == out.stat().st_size


def test_csv_empty_file_and_header_only(work):
    for i, text in enumerate(("", "﻿", "source_id,event_id,a\n")):
        sub = work / str(i)
        sub.mkdir()
        rc, out, cp, _, _ = run_csv(sub, text)
        assert rc == 0
        assert read_jsonl(out) == []


@pytest.mark.parametrize(
    "csv_text,needle",
    [
        ("source_id,source_id,event_id\n", "duplicate"),
        ("source_id,,event_id\n", "empty column"),
        ("source_id,name\nc,e1,x\n", "missing required column"),
        ("event_id,source_id\ne1,c\n", None),  # valid: order irrelevant
        ("source_id,event_id\n\n", "empty logical record"),
        ("source_id,event_id,a\nc,e1\n", "expected 3 columns, got 2"),
        ("source_id,event_id\nc,e1,x\n", "expected 2 columns, got 3"),
        ('source_id,event_id\nc,"unclosed\n', "unterminated"),
        ("source_id,event_id\nx,e1\n", "does not match"),
        ("source_id,event_id\nc,\n", "event_id"),
        ("source_id,event_id\n﻿c,e1\n", "BOM"),
    ],
)
def test_csv_validation_errors(work, capsys, csv_text, needle):
    rc, out, cp, _, _ = run_csv(work, csv_text)
    if needle is None:
        assert rc == 0
        return
    assert rc == 3
    err = capsys.readouterr().err
    assert err.startswith("Error: DataValidationError: ")
    assert needle in err


def test_csv_invalid_utf8(work, capsys):
    src = work / "c.csv"
    cfg = work / "c.yaml"
    cfg.write_text(CSV_CFG.format(src=str(src)), encoding="utf-8")
    with open(src, "wb") as fp:
        fp.write(b"source_id,event_id,x\nc,e1,\xff\xfe\n")
    rc = cli_main(["run", "-c", str(cfg), "-o", str(work / "o"),
                   "-p", str(work / "c")])
    assert rc == 3
    assert "UTF-8" in capsys.readouterr().err


def test_csv_transforms_and_schema_version(work):
    extra = ("transforms:\n  - op: cast\n    field: n\n    type: integer\n")
    rc, out, cp, _, _ = run_csv(
        work,
        "source_id,event_id,n,keep\n"
        "c,1,1,a\n"
        "c,2,2,b\n"   # value change only: no new version
        "c,3,3,c\n",
        extra_cfg=extra,
    )
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["data"]["n"] for r in rows] == [1, 2, 3]
    assert all(isinstance(r["data"]["n"], int) for r in rows)
    assert [r["schema_version"] for r in rows] == [1, 1, 1]


def test_csv_failed_record_leaves_no_partial_commit(work, capsys):
    # batch_size 2: records 1,2 commit; record 3 is malformed, its batch
    # (and the record itself) must not be committed.
    rc, out, cp, cfg, src = run_csv(
        work,
        "source_id,event_id,a\n"
        "c,1,x\n"
        "c,2,y\n"
        "c,3,z,extra\n",
    )
    assert rc == 3
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2"]
    assert read_checkpoint(cp)["sources"]["c"]["records"] == 2

    # repair the input and replay: continue at the failed logical record
    write_csv(src, "source_id,event_id,a\nc,1,x\nc,2,y\nc,3,z\n")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2", "3"]
    assert read_checkpoint(cp)["sources"]["c"]["records"] == 3


def test_csv_replay_appends_and_is_idempotent(work):
    rc, out, cp, cfg, src = run_csv(
        work, "source_id,event_id,a\nc,1,x\nc,2,y\n"
    )
    assert rc == 0
    with open(src, "a", encoding="utf-8") as fp:
        fp.write('c,3,"multi\nline"\nc,4,z\n')
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["event_id"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[2]["data"] == {"a": "multi\nline"}

    size = out.stat().st_size
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    assert out.stat().st_size == size
    assert len(read_jsonl(out)) == 4


def test_csv_replay_truncates_uncommitted_tail(work):
    rc, out, cp, cfg, src = run_csv(
        work, "source_id,event_id,a\nc,1,x\nc,2,y\n"
    )
    assert rc == 0
    with open(out, "ab") as fp:
        fp.write(b'{"source_id":"c","event_id":"ghost","schema_version":9,'
                 b'"data":{}}\n')
    with open(src, "a", encoding="utf-8") as fp:
        fp.write("c,3,z\n")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 0
    assert [r["event_id"] for r in read_jsonl(out)] == ["1", "2", "3"]
    assert read_checkpoint(cp)["sink_offset"] == out.stat().st_size


def test_csv_replay_rejects_changed_prefix(work, capsys):
    rc, out, cp, cfg, src = run_csv(
        work, "source_id,event_id,a\nc,1,x\nc,2,y\n"
    )
    assert rc == 0
    # rewrite a committed byte, keeping the file length identical
    write_csv(src, "source_id,event_id,a\nc,1,q\nc,2,y\nc,3,z\n")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "cannot recover" in capsys.readouterr().err


def test_csv_replay_rejects_shrunk_input(work, capsys):
    rc, out, cp, cfg, src = run_csv(
        work, "source_id,event_id,a\nc,1,x\nc,2,y\n"
    )
    assert rc == 0
    write_csv(src, "source_id,event_id\n")
    rc = cli_main(["replay", "-c", str(cfg), "-o", str(out), "-p", str(cp)])
    assert rc == 4
    assert "cannot recover" in capsys.readouterr().err


MIXED_CFG = """
sources:
  - id: c
    type: csv
    path: {c}
    batch_size: 2
  - id: j
    type: jsonl
    path: {j}
    batch_size: 2
transforms:
  - op: set
    field: seen
    value: ok
"""


def test_csv_and_jsonl_sources_mixed_in_order(work):
    c = work / "c.csv"
    j = work / "j.jsonl"
    cfg = work / "c.yaml"
    cfg.write_text(MIXED_CFG.format(c=str(c), j=str(j)), encoding="utf-8")
    write_csv(c, "source_id,event_id,k\nc,1,a\nc,2,b\n")
    write_jsonl(j, [env_record("j", 1, {"k": "x"}),
                    env_record("j", 2, {"k": "y"})])
    out, cp = work / "o.jsonl", work / "cp.json"
    assert cli_main(["run", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("c", "1"), ("c", "2"), ("j", 1), ("j", 2),
    ]
    assert rows[0]["data"] == {"k": "a", "seen": "ok"}
    assert rows[2]["data"] == {"k": "x", "seen": "ok"}

    # append to both inputs; replay resumes each source independently
    with open(c, "a", encoding="utf-8") as fp:
        fp.write("c,3,d\n")
    with open(j, "a", encoding="utf-8") as fp:
        fp.write(json.dumps(env_record("j", 3, {"k": "z"})) + "\n")
    assert cli_main(["replay", "-c", str(cfg), "-o", str(out),
                     "-p", str(cp)]) == 0
    rows = read_jsonl(out)
    assert [(r["source_id"], r["event_id"]) for r in rows] == [
        ("c", "1"), ("c", "2"), ("j", 1), ("j", 2), ("c", "3"), ("j", 3),
    ]
    doc = read_checkpoint(cp)
    assert doc["sources"]["c"]["records"] == 3
    assert doc["sources"]["j"]["records"] == 3


def test_csv_source_level_transforms(work):
    cfg_text = """
sources:
  - id: c
    type: csv
    path: {src}
    batch_size: 5
    transforms:
      - op: cast
        field: total
        type: number
transforms:
  - op: rename
    from: amount
    to: total
"""
    rc, out, cp, _, _ = run_csv(
        work, "source_id,event_id,amount\nc,1,10.5\nc,2,2\n",
        cfg_text=cfg_text,
    )
    assert rc == 0
    rows = read_jsonl(out)
    assert [r["data"] for r in rows] == [{"total": 10.5}, {"total": 2.0}]
    assert [r["schema_version"] for r in rows] == [1, 1]
