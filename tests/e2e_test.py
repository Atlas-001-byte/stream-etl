#!/usr/bin/env python3
"""端到端验证脚本（不使用测试框架，直接断言并汇总）。"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "stream-etl")

passed = 0
failed = 0


def run(args, expect_code, stdin=None):
    proc = subprocess.run(
        [BIN] + args,
        input=stdin,
        capture_output=True,
        text=True,
        cwd=TMP,
    )
    return proc


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print("  PASS  %s" % name)
    else:
        failed += 1
        print("  FAIL  %s  %s" % (name, detail))


def write(path, content):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def read_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


TMP = tempfile.mkdtemp(prefix="stream-etl-test-")
os.chdir(TMP)
print("工作目录: %s" % TMP)

try:
    # ---------- 1. 成功路径：多 source + 全部 transforms + schema 演进 ----------
    print("\n[1] run 成功路径与 schema_version 演进")
    write("a.jsonl", "\n".join([
        json.dumps({"source_id": "srcA", "event_id": "e1",
                    "payload": {"x": 1, "name": "n1", "deep": {"city": "Berlin"}, "score": "10"}}),
        json.dumps({"source_id": "srcA", "event_id": "e2",
                    "payload": {"x": 2, "name": "n2", "deep": {"city": "Paris"}, "score": "20"}}),
        json.dumps({"source_id": "srcA", "event_id": "e3",
                    "payload": {"x": 3, "name": "n3", "deep": {"city": "Rome", "zip": "00100"}, "score": "30"}}),
        json.dumps({"source_id": "srcA", "event_id": "e4",
                    "payload": {"x": 4, "name": "n4", "deep": {"city": "Oslo"}, "score": 40}}),
    ]) + "\n")
    write("b.jsonl", "\n".join([
        json.dumps({"source_id": "srcB", "event_id": "f1",
                    "payload": {"x": 1, "name": "k1", "deep": {"city": "X"}, "score": "1"}}),
        json.dumps({"source_id": "srcB", "event_id": "f2",
                    "payload": {"x": 2, "name": "k2", "deep": {"city": "Y"}, "score": "2"}}),
    ]) + "\n")
    write("config.yaml", """\
sources:
  - id: srcA
    type: jsonl
    path: a.jsonl
    batch_size: 2
  - id: srcB
    type: jsonl
    path: b.jsonl
    batch_size: 5
transforms:
  - op: rename
    from: name
    to: label
  - op: rename
    from: deep.city
    to: deep.town
  - op: drop
    path: x
  - op: set
    path: meta.kind
    value: order
  - op: cast
    path: score
    type: integer
""")
    p = run(["run", "--config", "config.yaml", "--output", "out.jsonl",
             "--checkpoint", "cp.json"], 0)
    check("run 退出码 0", p.returncode == 0, p.stderr)
    check("成功时 stdout 为空", p.stdout == "", repr(p.stdout))
    check("输出文件存在", os.path.exists("out.jsonl"))
    check("检查点存在", os.path.exists("cp.json"))

    rows = read_jsonl("out.jsonl")
    check("共输出 6 条", len(rows) == 6, str(len(rows)))
    check("按 source 顺序：前 4 条 srcA",
          [r["source_id"] for r in rows] == ["srcA"] * 4 + ["srcB"] * 2)
    check("输出字段恰好为四元组",
          all(set(r) == {"source_id", "event_id", "schema_version", "data"} for r in rows))
    check("rename 顶层字段生效", all("label" in r["data"] and "name" not in r["data"] for r in rows[:4]))
    check("rename 嵌套字段生效", all("town" in r["data"]["deep"] and "city" not in r["data"]["deep"] for r in rows[:4]))
    check("drop 生效", all("x" not in r["data"] for r in rows[:4]))
    check("set 嵌套路径生效", all(r["data"]["meta"]["kind"] == "order" for r in rows[:4]))
    check("cast string->integer", rows[0]["data"]["score"] == 10 and isinstance(rows[0]["data"]["score"], int))
    check("cast integer->integer", rows[3]["data"]["score"] == 40)
    check("schema_version 初值 1", rows[0]["schema_version"] == 1)
    check("同构不递增(score 10/20 同型)", rows[1]["schema_version"] == 1)
    check("新增字段 zip 递增到 2", rows[2]["schema_version"] == 2, str(rows[2]["schema_version"]))
    check("字段删除回退 递增到 3", rows[3]["schema_version"] == 3, str(rows[3]["schema_version"]))
    check("srcB 独立从 1 开始", [r["schema_version"] for r in rows[4:]] == [1, 1])
    cp = json.load(open("cp.json"))
    check("检查点记录 srcA 已提交 4",
          cp["sources"][0]["committed_records"] == 4 and cp["sources"][0]["schema_version"] == 3)
    check("检查点记录 srcB 已提交 2",
          cp["sources"][1]["committed_records"] == 2 and cp["sources"][1]["schema_version"] == 1)

    # 幂等：完成后 replay，没有新增记录
    p = run(["replay", "--config", "config.yaml", "--output", "out.jsonl",
             "--checkpoint", "cp.json"], 0)
    check("已完成后 replay 退出码 0", p.returncode == 0, p.stderr)
    check("已完成后 replay 不重复输出", len(read_jsonl("out.jsonl")) == 6)

    # run 拒绝已存在的输出/检查点
    p = run(["run", "--config", "config.yaml", "--output", "out.jsonl",
             "--checkpoint", "cp2.json"], 2)
    check("run 拒绝已存在输出 -> ConfigurationError(2)",
          p.returncode == 2 and "ConfigurationError" in p.stderr, p.stderr)

    # ---------- 2. replay：批次边界后崩溃，精确一次重放 ----------
    print("\n[2] replay 从未提交批次继续（截断 + 不重复不跳过）")
    shutil.rmtree(TMP)
    os.makedirs(TMP)
    os.chdir(TMP)
    write("a.jsonl", "\n".join([
        json.dumps({"source_id": "s", "event_id": "e%d" % i,
                    "payload": {"v": i}}) for i in range(1, 8)
    ]) + "\n")
    write("config.yaml", """\
sources:
  - id: s
    type: jsonl
    path: a.jsonl
    batch_size: 3
""")
    # 初始输入只有前 5 行；run 后手工构造“崩溃现场”：检查点停在 3，输出保留 5 行
    write("a.jsonl", "\n".join([
        json.dumps({"source_id": "s", "event_id": "e%d" % i,
                    "payload": {"v": i}}) for i in range(1, 6)
    ]) + "\n")
    p = run(["run", "--config", "config.yaml", "--output", "o5.jsonl",
             "--checkpoint", "c5.json"], 0)
    assert p.returncode == 0, p.stderr
    # 篡改崩溃现场：检查点回退到 3，输出保留全部 5 行（后 2 行视为未提交残留）
    cp5 = json.load(open("c5.json"))
    cp5["sources"][0]["committed_records"] = 3
    write("c5.json", json.dumps(cp5))
    # 输入增长到 7 行（新数据到达），路径保持不变
    write("a.jsonl", "\n".join([
        json.dumps({"source_id": "s", "event_id": "e%d" % i,
                    "payload": {"v": i}}) for i in range(1, 8)
    ]) + "\n")
    p = run(["replay", "--config", "config.yaml", "--output", "o5.jsonl",
             "--checkpoint", "c5.json"], 0)
    check("replay 退出码 0", p.returncode == 0, p.stderr)
    rows = read_jsonl("o5.jsonl")
    ids = [r["event_id"] for r in rows]
    check("replay 后恰好 7 条且无重复无跳过",
          ids == ["e1", "e2", "e3", "e4", "e5", "e6", "e7"], str(ids))
    check("replay 后检查点推进到 7",
          json.load(open("c5.json"))["sources"][0]["committed_records"] == 7)
    # 再 replay 一次保持不变
    p = run(["replay", "--config", "config.yaml", "--output", "o5.jsonl",
             "--checkpoint", "c5.json"], 0)
    check("重复 replay 幂等",
          [r["event_id"] for r in read_jsonl("o5.jsonl")] ==
          ["e1", "e2", "e3", "e4", "e5", "e6", "e7"])

    # replay 要求输出与检查点都已存在
    p = run(["replay", "--config", "config.yaml", "--output", "nope.jsonl",
             "--checkpoint", "c5.json"], 4)
    check("replay 输出不存在 -> CheckpointError(4)",
          p.returncode == 4 and "CheckpointError" in p.stderr, p.stderr)
    p = run(["replay", "--config", "config.yaml", "--output", "o5.jsonl",
             "--checkpoint", "nope.json"], 4)
    check("replay 检查点不存在 -> CheckpointError(4)",
          p.returncode == 4 and "CheckpointError" in p.stderr, p.stderr)

    # 检查点损坏 / 版本不匹配
    write("bad.json", "{ not json")
    p = run(["replay", "--config", "config.yaml", "--output", "o5.jsonl",
             "--checkpoint", "bad.json"], 4)
    check("损坏检查点 -> CheckpointError(4)",
          p.returncode == 4 and "CheckpointError" in p.stderr, p.stderr)
    badver = dict(cp5)
    badver["format_version"] = 99
    write("ver.json", json.dumps(badver))
    p = run(["replay", "--config", "config.yaml", "--output", "o5.jsonl",
             "--checkpoint", "ver.json"], 4)
    check("版本不匹配 -> CheckpointError(4)",
          p.returncode == 4 and "CheckpointError" in p.stderr, p.stderr)
    # 输出记录数少于检查点 -> 无法恢复
    p = run(["run", "--config", "config.yaml", "--output", "small.jsonl",
             "--checkpoint", "smallcp.json"], 0)
    write("small.jsonl", "\n".join(open("small.jsonl").read().splitlines()[:2]) + "\n")
    p = run(["replay", "--config", "config.yaml", "--output", "small.jsonl",
             "--checkpoint", "smallcp.json"], 4)
    check("输出短于检查点 -> CheckpointError(4)",
          p.returncode == 4 and "CheckpointError" in p.stderr, p.stderr)

    # ---------- 3. 配置错误（退出码 2） ----------
    print("\n[3] ConfigurationError -> 2")
    def cfg_err(name, text, extra_args=None):
        write("badcfg.yaml", text)
        args = ["run", "--config", "badcfg.yaml", "--output", "x.jsonl",
                "--checkpoint", "x.json"]
        if extra_args:
            args = extra_args
        p = run(args, 2)
        check(name, p.returncode == 2 and "ConfigurationError" in p.stderr, p.stderr)

    src_ok = ("sources:\n"
              "  - id: x\n"
              "    type: jsonl\n"
              "    path: a.jsonl\n"
              "    batch_size: 1\n")
    cfg_err("source 缺字段", "sources:\n  - id: x\n    type: jsonl\n")
    cfg_err("未知 source type", "sources:\n  - id: x\n    type: csv\n    path: a.jsonl\n    batch_size: 1\n")
    cfg_err("未知操作", src_ok + "transforms:\n  - op: frobnicate\n    path: q\n")
    cfg_err("cast 非法类型", src_ok + "transforms:\n  - op: cast\n    path: q\n    type: float\n")
    cfg_err("非法路径（空段）", src_ok + "transforms:\n  - op: drop\n    path: a..b\n")
    cfg_err("未知顶层键", src_ok + "unknown: 1\n")
    cfg_err("source id 重复", "sources:\n"
            "  - id: x\n    type: jsonl\n    path: a.jsonl\n    batch_size: 1\n"
            "  - id: x\n    type: jsonl\n    path: a.jsonl\n    batch_size: 1\n")
    cfg_err("配置文件不存在", "", ["run", "--config", "missing.yaml",
                              "--output", "x.jsonl", "--checkpoint", "x.json"])
    cfg_err("YAML 语法错误", "sources:\n  - id: x\n   bad indent: 1\n")
    cfg_err("未知子命令", "", ["frobnicate"])
    cfg_err("缺少必填参数", "", ["run", "--config", "config.yaml"])
    cfg_err("batch_size 非法", "sources:\n  - id: x\n    type: jsonl\n    path: a.jsonl\n    batch_size: 0\n")

    # ---------- 4. 数据错误（退出码 3），当前批次不提交 ----------
    print("\n[4] DataValidationError -> 3，失败批次不提交")
    write("d.jsonl", "\n".join([
        json.dumps({"source_id": "s", "event_id": "e1", "payload": {"v": 1}}),
        json.dumps({"source_id": "s", "event_id": "e2", "payload": {"v": 2}}),
        json.dumps({"source_id": "s", "event_id": "e3", "payload": {"v": 3}}),
        "not-a-json-line",
    ]) + "\n")
    write("dcfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: d.jsonl
    batch_size: 2
""")
    p = run(["run", "--config", "dcfg.yaml", "--output", "dout.jsonl",
             "--checkpoint", "dcp.json"], 3)
    check("非法 JSON 行 -> DataValidationError(3)",
          p.returncode == 3 and "DataValidationError" in p.stderr, p.stderr)
    # 前 2 条已提交（第 1 批），失败发生在第 2 批；输出里可能残留 e3，但检查点停在 2
    cp_d = json.load(open("dcp.json"))
    check("失败批次不提交（检查点停在 2）",
          cp_d["sources"][0]["committed_records"] == 2, str(cp_d))
    check("已提交的 2 条在输出中",
          [r["event_id"] for r in read_jsonl("dout.jsonl")[:2]] == ["e1", "e2"])

    # 修复输入后 replay：截断残留 e3，重新处理 e3、e4
    write("d.jsonl", "\n".join([
        json.dumps({"source_id": "s", "event_id": "e1", "payload": {"v": 1}}),
        json.dumps({"source_id": "s", "event_id": "e2", "payload": {"v": 2}}),
        json.dumps({"source_id": "s", "event_id": "e3", "payload": {"v": 3}}),
        json.dumps({"source_id": "s", "event_id": "e4", "payload": {"v": 4}}),
    ]) + "\n")
    p = run(["replay", "--config", "dcfg.yaml", "--output", "dout.jsonl",
             "--checkpoint", "dcp.json"], 0)
    check("修复后 replay 成功且精确一次",
          [r["event_id"] for r in read_jsonl("dout.jsonl")] ==
          ["e1", "e2", "e3", "e4"], p.stderr)

    # 缺 source_id / event_id / payload
    write("m.jsonl", json.dumps({"event_id": "e", "payload": {}}) + "\n")
    write("mcfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: m.jsonl
    batch_size: 2
""")
    p = run(["run", "--config", "mcfg.yaml", "--output", "mout.jsonl",
             "--checkpoint", "mcp.json"], 3)
    check("缺 source_id -> DataValidationError(3)",
          p.returncode == 3 and "DataValidationError" in p.stderr, p.stderr)

    # 路径不存在（drop / cast / rename from）
    write("p.jsonl", json.dumps({"source_id": "s", "event_id": "e",
                                 "payload": {"a": 1}}) + "\n")
    write("pcfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: p.jsonl
    batch_size: 2
transforms:
  - op: drop
    path: a.b.c
""")
    p = run(["run", "--config", "pcfg.yaml", "--output", "pout.jsonl",
             "--checkpoint", "pcp.json"], 3)
    check("drop 路径不存在 -> DataValidationError(3)",
          p.returncode == 3 and "DataValidationError" in p.stderr, p.stderr)

    # cast 失败：字符串 abc 无法转 integer
    write("ccfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: p.jsonl
    batch_size: 2
transforms:
  - op: set
    path: q
    value: abc
  - op: cast
    path: q
    type: integer
""")
    p = run(["run", "--config", "ccfg.yaml", "--output", "cout.jsonl",
             "--checkpoint", "ccp.json"], 3)
    check("cast 失败 -> DataValidationError(3)",
          p.returncode == 3 and "DataValidationError" in p.stderr, p.stderr)

    # set 路径中段是标量（会静默覆盖字段）-> DataValidationError
    write("ocfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: p.jsonl
    batch_size: 2
transforms:
  - op: set
    path: a.b
    value: x
""")
    p = run(["run", "--config", "ocfg.yaml", "--output", "oout.jsonl",
             "--checkpoint", "ocp.json"], 3)
    check("set 路径中段不是对象 -> DataValidationError(3)",
          p.returncode == 3 and "DataValidationError" in p.stderr, p.stderr)

    # ---------- 5. I/O 错误（退出码 5） ----------
    print("\n[5] SourceError / SinkError -> 5")
    write("iocfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: does-not-exist.jsonl
    batch_size: 2
""")
    p = run(["run", "--config", "iocfg.yaml", "--output", "io.jsonl",
             "--checkpoint", "io.json"], 5)
    check("输入不存在 -> SourceError(5)",
          p.returncode == 5 and "SourceError" in p.stderr, p.stderr)
    # 输出路径的父级是普通文件 -> 无法创建 -> SinkError
    write("afile", "i am a file\n")
    p = run(["run", "--config", "dcfg.yaml", "--output", "afile/o.jsonl",
             "--checkpoint", "afile/c.json"], 5)
    check("输出路径不可创建 -> SinkError(5)",
          p.returncode == 5 and "SinkError" in p.stderr, p.stderr)

    # 输入是目录
    os.makedirs("adir", exist_ok=True)
    write("dircfg.yaml", """\
sources:
  - id: s
    type: jsonl
    path: adir
    batch_size: 2
""")
    p = run(["run", "--config", "dircfg.yaml", "--output", "diro.jsonl",
             "--checkpoint", "dirc.json"], 5)
    check("输入路径是目录 -> SourceError(5)",
          p.returncode == 5 and "SourceError" in p.stderr, p.stderr)

finally:
    def _on_error(func, path, exc_info):
        os.chmod(path, 0o700)
        func(path)
    shutil.rmtree(TMP, onerror=_on_error)

print("\n结果: %d 通过, %d 失败" % (passed, failed))
sys.exit(1 if failed else 0)
