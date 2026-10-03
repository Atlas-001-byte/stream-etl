"""ETL 引擎：run / replay、schema 演进与检查点提交。

提交语义（每条 source 独立计数，按配置中的 source 顺序处理）：

1. 记录转换后追加写入输出文件并 flush；
2. 每累计 ``batch_size`` 条（或一个 source 处理结束）原子替换检查点；
3. 崩溃后 replay：输出截断到检查点中已提交的行数，输入按行跳过已提交部分，
   未提交记录重新处理 —— 不重复、不跳过。
"""

import json
import os
import tempfile

from .errors import (
    CheckpointError,
    ConfigurationError,
    DataValidationError,
    SinkError,
    SourceError,
)
from .schema import (
    PathCollision,
    PathNotFound,
    cast_value,
    get_path,
    remove_path,
    set_path,
    type_name,
)

CHECKPOINT_FORMAT = "stream-etl-checkpoint"
CHECKPOINT_FORMAT_VERSION = 1


def _check_sources_readable(config):
    for source in config.sources:
        if os.path.isdir(source.path):
            raise SourceError("输入路径是目录，不可读: %s" % source.path)
        if not os.path.exists(source.path):
            raise SourceError("输入文件不存在: %s" % source.path)
        if not os.access(source.path, os.R_OK):
            raise SourceError("输入文件不可读: %s" % source.path)


class SourceState:
    def __init__(self):
        self.committed_records = 0
        self.schema_version = 0
        self.fingerprint = None
        self.processed = 0  # 已消费的输入行数（含跳过的已提交行）


def _fingerprint(data):
    """data 的字段签名：点分路径 -> 叶子类型名，嵌套对象内序展开。"""
    sig = {}

    def walk(node, prefix):
        for key in node:
            path = prefix + "." + key if prefix else key
            value = node[key]
            if isinstance(value, dict):
                walk(value, path)
            else:
                sig[path] = type_name(value)

    walk(data, "")
    return json.dumps(sig, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _apply_transforms(data, transforms):
    for t in transforms:
        if t.op == "rename":
            try:
                value = get_path(data, t.from_segments)
            except PathNotFound:
                raise DataValidationError(
                    "rename 的 from 路径不存在: %s" % ".".join(t.from_segments)
                )
            try:
                remove_path(data, t.from_segments)
            except PathNotFound:
                raise DataValidationError(
                    "rename 的 from 路径不存在: %s" % ".".join(t.from_segments)
                )
            try:
                set_path(data, t.to_segments, value)
            except PathCollision:
                raise DataValidationError(
                    "rename 的 to 路径中段不是对象: %s" % ".".join(t.to_segments)
                )
        elif t.op == "drop":
            try:
                remove_path(data, t.path_segments)
            except PathNotFound:
                raise DataValidationError(
                    "drop 的路径不存在: %s" % ".".join(t.path_segments)
                )
        elif t.op == "set":
            try:
                set_path(data, t.path_segments, t.value)
            except PathCollision:
                raise DataValidationError(
                    "set 的路径中段不是对象: %s" % ".".join(t.path_segments)
                )
        elif t.op == "cast":
            try:
                current = get_path(data, t.path_segments)
            except PathNotFound:
                raise DataValidationError(
                    "cast 的路径不存在: %s" % ".".join(t.path_segments)
                )
            set_path(data, t.path_segments, cast_value(current, t.target_type))


def _parse_record(line, lineno):
    try:
        record = json.loads(line)
    except ValueError as exc:
        raise DataValidationError("第 %d 行不是合法 JSON: %s" % (lineno, exc)) from exc
    if not isinstance(record, dict):
        raise DataValidationError("第 %d 行记录必须是 JSON 对象" % lineno)
    missing = [k for k in ("source_id", "event_id", "payload") if k not in record]
    if missing:
        raise DataValidationError("第 %d 行记录缺少字段: %s" % (lineno, ", ".join(missing)))
    if not isinstance(record["payload"], dict):
        raise DataValidationError("第 %d 行 payload 必须是对象" % lineno)
    return record


class Engine:
    def __init__(self, config, output_path, checkpoint_path):
        self.config = config
        self.output_path = output_path
        self.checkpoint_path = checkpoint_path
        self.sink = None
        self.states = {s.id: SourceState() for s in config.sources}
        self.pending = 0  # 自上次提交以来写出的记录数

    # ---------- 检查点 ----------

    def _checkpoint_payload(self):
        return {
            "format": CHECKPOINT_FORMAT,
            "format_version": CHECKPOINT_FORMAT_VERSION,
            "sources": [
                {
                    "id": s.id,
                    "path": s.path,
                    "committed_records": st.committed_records,
                    "schema_version": st.schema_version,
                    "fingerprint": st.fingerprint,
                }
                for s, st in (
                    (src, self.states[src.id]) for src in self.config.sources
                )
            ],
        }

    def _commit(self):
        # 先推进内存中的已提交计数，再持久化；持久化失败整个进程以错误退出，
        # 内存状态随后丢弃，不会产生“已推进但未持久化”的外部可观察后果。
        for st in self.states.values():
            st.committed_records = st.processed
        payload = self._checkpoint_payload()
        try:
            self.sink.flush()
            os.fsync(self.sink.fileno())
        except OSError as exc:
            raise SinkError("输出文件 flush 失败: %s" % exc) from exc
        directory = os.path.dirname(os.path.abspath(self.checkpoint_path)) or "."
        tmp_fd = None
        tmp_name = None
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix=".checkpoint-", suffix=".tmp", dir=directory
            )
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                tmp_fd = None
                json.dump(payload, fh, ensure_ascii=False)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, self.checkpoint_path)
            tmp_name = None
            try:
                dir_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass  # 目录 fsync 是尽力而为
        except OSError as exc:
            raise SinkError("检查点写入失败: %s" % exc) from exc
        finally:
            if tmp_fd is not None:
                os.close(tmp_fd)
            if tmp_name is not None:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
        self.pending = 0

    def _load_checkpoint(self):
        try:
            with open(self.checkpoint_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, ValueError) as exc:
            raise CheckpointError("检查点损坏或不可读: %s" % exc) from exc
        try:
            if not isinstance(raw, dict):
                raise ValueError("根节点必须是对象")
            if raw.get("format") != CHECKPOINT_FORMAT:
                raise CheckpointError(
                    "检查点版本不匹配: format=%r" % raw.get("format")
                )
            if raw.get("format_version") != CHECKPOINT_FORMAT_VERSION:
                raise CheckpointError(
                    "检查点版本不匹配: format_version=%r"
                    % raw.get("format_version")
                )
            entries = raw["sources"]
            if not isinstance(entries, list) or len(entries) != len(self.config.sources):
                raise ValueError("sources 数量与配置不一致")
            by_id = {}
            for entry in entries:
                if not isinstance(entry, dict):
                    raise ValueError("source 条目必须是对象")
                sid = entry["id"]
                if not isinstance(sid, str):
                    raise ValueError("source id 必须是字符串")
                if sid in by_id:
                    raise ValueError("source id 重复: %s" % sid)
                by_id[sid] = entry
            for src in self.config.sources:
                entry = by_id.get(src.id)
                if entry is None:
                    raise CheckpointError("检查点缺少 source: %s" % src.id)
                if entry.get("path") != src.path:
                    raise CheckpointError(
                        "source %s 的输入路径与检查点不一致" % src.id
                    )
                count = entry.get("committed_records")
                version = entry.get("schema_version")
                fp = entry.get("fingerprint")
                if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                    raise ValueError("committed_records 必须是非负整数")
                if not isinstance(version, int) or isinstance(version, bool) or version < 0:
                    raise ValueError("schema_version 必须是非负整数")
                if fp is not None and not isinstance(fp, str):
                    raise ValueError("fingerprint 必须是字符串或 null")
                if (version == 0) != (fp is None) or (count == 0 and version != 0):
                    raise ValueError("schema_version / fingerprint / 计数不一致")
                st = self.states[src.id]
                st.committed_records = count
                st.processed = count
                st.schema_version = version
                st.fingerprint = fp
        except CheckpointError:
            raise
        except ValueError as exc:
            raise CheckpointError("检查点损坏: %s" % exc) from exc
        total = sum(st.committed_records for st in self.states.values())
        return total

    # ---------- 输出 ----------

    def _truncate_output_to_committed(self, committed_total):
        """把已有输出截断到已提交行数；返回时句柄定位在追加位置。"""
        try:
            fh = open(self.output_path, "r+b")
        except OSError as exc:
            raise SinkError("输出文件不可读写: %s" % exc) from exc
        counted = 0
        try:
            if committed_total == 0:
                fh.seek(0)
                fh.truncate()
                fh.flush()
                os.fsync(fh.fileno())
                fh.seek(0, os.SEEK_END)
                self.sink = fh
                return
            while counted < committed_total:
                line = fh.readline()
                if line == b"":
                    raise CheckpointError(
                        "输出记录数(%d)少于检查点已提交数(%d)，无法恢复"
                        % (counted, committed_total)
                    )
                if not line.endswith(b"\n"):
                    raise CheckpointError("输出文件第 %d 行不完整，无法恢复" % (counted + 1))
                if line.strip() == b"":
                    raise CheckpointError("输出文件含空行，检查点无法对齐")
                try:
                    obj = json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeDecodeError) as exc:
                    raise CheckpointError("输出文件记录损坏，无法恢复: %s" % exc) from exc
                if not isinstance(obj, dict) or not (
                    {"source_id", "event_id", "schema_version", "data"} <= set(obj)
                ):
                    raise CheckpointError("输出文件记录结构无法识别，无法恢复")
                counted += 1
            # 之后的内容（含半行）全部属于未提交残留，直接丢弃。
            fh.truncate()
            fh.flush()
            os.fsync(fh.fileno())
            fh.seek(0, os.SEEK_END)
        except CheckpointError:
            fh.close()
            raise
        except OSError as exc:
            fh.close()
            raise SinkError("输出文件恢复失败: %s" % exc) from exc
        self.sink = fh

    def _open_fresh_output(self):
        directory = os.path.dirname(os.path.abspath(self.output_path)) or "."
        if not os.path.isdir(directory) or not os.access(directory, os.W_OK):
            raise SinkError("输出目录不存在或不可写: %s" % directory)
        try:
            self.sink = open(self.output_path, "wb")
        except OSError as exc:
            raise SinkError("输出文件不可写: %s" % exc) from exc

    def _write_record(self, source, state, record, data):
        fp = _fingerprint(data)
        if state.schema_version == 0:
            state.schema_version = 1
        elif fp != state.fingerprint:
            state.schema_version += 1
        state.fingerprint = fp
        out = {
            "source_id": record["source_id"],
            "event_id": record["event_id"],
            "schema_version": state.schema_version,
            "data": data,
        }
        try:
            self.sink.write(
                (json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
                    "utf-8"
                )
            )
        except (OSError, UnicodeError) as exc:
            raise SinkError("输出文件写入失败: %s" % exc) from exc
        self.pending += 1
        if self.pending >= source.batch_size:
            self._commit()

    # ---------- 主流程 ----------

    def _process_source(self, source, skip):
        state = self.states[source.id]
        try:
            fh = open(source.path, "r", encoding="utf-8")
        except OSError as exc:
            raise SourceError("输入文件不可读: %s: %s" % (source.path, exc)) from exc
        try:
            lineno = 0
            for raw_line in fh:
                lineno += 1
                if lineno <= skip:
                    continue
                line = raw_line.strip()
                if line == "":
                    raise DataValidationError(
                        "%s 第 %d 行不是合法 JSON 对象（空行）"
                        % (source.path, lineno)
                    )
                record = _parse_record(line, lineno)
                data = record["payload"]
                _apply_transforms(data, self.config.transforms)
                state.processed += 1
                self._write_record(source, state, record, data)
            if lineno < skip:
                raise CheckpointError(
                    "输入文件 %s 行数(%d)少于检查点已提交数(%d)，无法恢复"
                    % (source.path, lineno, skip)
                )
        except UnicodeDecodeError as exc:
            raise DataValidationError(
                "%s 第 %d 行不是合法 UTF-8: %s" % (source.path, lineno, exc)
            ) from exc
        except OSError as exc:
            raise SourceError("输入文件读取失败: %s: %s" % (source.path, exc)) from exc
        finally:
            fh.close()
        if self.pending > 0:
            self._commit()

    def run(self):
        for path in (self.output_path, self.checkpoint_path):
            if os.path.exists(path):
                raise ConfigurationError(
                    "%s 已存在：run 要求全新的输出与检查点路径（续跑请用 replay）" % path
                )
        _check_sources_readable(self.config)
        self._open_fresh_output()
        # 先落一份零状态检查点：即便所有输入均为空，成功后检查点也必须存在；
        # 同时让“尚未处理任何记录就崩溃”也能用 replay 恢复。
        self._commit()
        try:
            for source in self.config.sources:
                self._process_source(source, skip=0)
        except BaseException:
            if self.sink is not None:
                self.sink.close()
            raise

    def replay(self):
        if not os.path.exists(self.output_path) or os.path.isdir(self.output_path):
            raise CheckpointError("输出文件不存在，无法 replay: %s" % self.output_path)
        if not os.path.exists(self.checkpoint_path) or os.path.isdir(self.checkpoint_path):
            raise CheckpointError(
                "检查点不存在，无法 replay: %s" % self.checkpoint_path
            )
        committed_total = self._load_checkpoint()
        _check_sources_readable(self.config)
        self._truncate_output_to_committed(committed_total)
        try:
            for source in self.config.sources:
                skip = self.states[source.id].committed_records
                self._process_source(source, skip=skip)
        except BaseException:
            if self.sink is not None:
                self.sink.close()
            raise
