"""配置加载与校验。

配置为 YAML，形如::

    sources:
      - id: orders
        type: jsonl
        path: data/orders.jsonl
        batch_size: 2
    transforms:
      - op: rename
        from: a.b
        to: a.c
      - op: drop
        path: x
      - op: set
        path: y
        value: 1
      - op: cast
        path: z
        type: integer
"""

from .errors import ConfigurationError
from .schema import CAST_TYPES, parse_path
from .yaml_lite import YAMLParseError, parse as parse_yaml

_REQUIRED_SOURCE_KEYS = ("id", "type", "path", "batch_size")
_ALLOWED_OPS = {
    "rename": ("from", "to"),
    "drop": ("path",),
    "set": ("path", "value"),
    "cast": ("path", "type"),
}


class SourceConfig:
    def __init__(self, node, index):
        if not isinstance(node, dict):
            raise ConfigurationError("sources[%d] 必须是映射" % index)
        missing = [k for k in _REQUIRED_SOURCE_KEYS if k not in node]
        if missing:
            raise ConfigurationError("source 不完整，缺少: %s" % ", ".join(missing))
        sid = node["id"]
        if not isinstance(sid, str) or sid == "":
            raise ConfigurationError("sources[%d].id 必须是非空字符串" % index)
        stype = node["type"]
        if stype != "jsonl":
            raise ConfigurationError("未知 source type: %r（仅支持 jsonl）" % stype)
        path = node["path"]
        if not isinstance(path, str) or path == "":
            raise ConfigurationError("sources[%d].path 必须是非空字符串" % index)
        batch_size = node["batch_size"]
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
            raise ConfigurationError(
                "sources[%d].batch_size 必须是正整数" % index
            )
        self.id = sid
        self.type = stype
        self.path = path
        self.batch_size = batch_size
        self.index = index


class Transform:
    """一条有序变换。"""

    def __init__(self, node, index):
        if not isinstance(node, dict):
            raise ConfigurationError("transforms[%d] 必须是映射" % index)
        op = node.get("op")
        if not isinstance(op, str) or op not in _ALLOWED_OPS:
            raise ConfigurationError(
                "transforms[%d] 的操作未知: %r（支持 %s）"
                % (index, op, ", ".join(sorted(_ALLOWED_OPS)))
            )
        allowed = _ALLOWED_OPS[op]
        unknown = [k for k in node if k != "op" and k not in allowed]
        if unknown:
            raise ConfigurationError(
                "transforms[%d] (%s) 含未知参数: %s" % (index, op, ", ".join(unknown))
            )
        missing = [k for k in allowed if k not in node]
        if missing:
            raise ConfigurationError(
                "transforms[%d] (%s) 缺少参数: %s" % (index, op, ", ".join(missing))
            )
        self.op = op
        self.index = index

        if op == "rename":
            frm = node["from"]
            to = node["to"]
            if not isinstance(frm, str) or not isinstance(to, str):
                raise ConfigurationError("transforms[%d] rename 的 from/to 必须是字符串" % index)
            self.from_segments = parse_path(frm)
            self.to_segments = parse_path(to)
        elif op == "drop":
            self.path_segments = parse_path(node["path"])
        elif op == "set":
            raw = node["path"]
            if not isinstance(raw, str):
                raise ConfigurationError("transforms[%d] set 的 path 必须是字符串" % index)
            self.path_segments = parse_path(raw)
            self.value = node["value"]
        elif op == "cast":
            raw = node["path"]
            if not isinstance(raw, str):
                raise ConfigurationError("transforms[%d] cast 的 path 必须是字符串" % index)
            self.path_segments = parse_path(raw)
            target = node["type"]
            if not isinstance(target, str) or target not in CAST_TYPES:
                raise ConfigurationError(
                    "transforms[%d] cast 的 type 非法: %r（仅接受 %s）"
                    % (index, target, ", ".join(CAST_TYPES))
                )
            self.target_type = target


class Config:
    def __init__(self, sources, transforms):
        self.sources = sources
        self.transforms = transforms


def load_config(path):
    """读取、解析并严格校验配置文件。"""
    if not isinstance(path, str) or path == "":
        raise ConfigurationError("配置路径必须是非空字符串")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        raise ConfigurationError("无法读取配置文件 %s: %s" % (path, exc)) from exc
    try:
        raw = parse_yaml(text)
    except YAMLParseError as exc:
        raise ConfigurationError("配置 YAML 解析失败: %s" % exc) from exc

    if not isinstance(raw, dict):
        raise ConfigurationError("配置顶层必须是映射")
    for key in raw:
        if key not in ("sources", "transforms"):
            raise ConfigurationError("配置含未知键: %r（仅接受 sources、transforms）" % key)
    if "sources" not in raw:
        raise ConfigurationError("source 不完整，缺少: sources")

    sources_node = raw["sources"]
    if not isinstance(sources_node, list) or len(sources_node) == 0:
        raise ConfigurationError("sources 必须是非空序列")
    sources = [SourceConfig(n, i) for i, n in enumerate(sources_node)]
    seen = set()
    for src in sources:
        if src.id in seen:
            raise ConfigurationError("source id 重复: %s" % src.id)
        seen.add(src.id)

    transforms_node = raw.get("transforms", [])
    if transforms_node is None:
        transforms_node = []
    if not isinstance(transforms_node, list):
        raise ConfigurationError("transforms 必须是序列")
    transforms = [Transform(n, i) for i, n in enumerate(transforms_node)]

    return Config(sources, transforms)
