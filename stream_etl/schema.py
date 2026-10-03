"""字段路径与 cast 规则。

路径是点分字符串（如 ``address.city``），每段为非空且不含 ``.`` 的键，
始终相对于记录的 ``payload``（即输出的 ``data``）解析。
"""

from .errors import ConfigurationError, DataValidationError


class PathNotFound(Exception):
    """记录上的字段路径不存在（运行期数据错误）。"""


class PathCollision(Exception):
    """路径中间段存在但不是对象，无法继续深入。"""


def parse_path(text):
    """配置期解析路径，非法路径抛 ConfigurationError。"""
    if not isinstance(text, str) or text == "":
        raise ConfigurationError("路径必须是非空字符串: %r" % (text,))
    segments = text.split(".")
    for seg in segments:
        if seg == "":
            raise ConfigurationError("路径 %r 含空段" % text)
    return segments


def get_path(root, segments):
    """按路径取值；任一键缺失或中途不是对象时抛 PathNotFound。"""
    cur = root
    for seg in segments:
        if not isinstance(cur, dict) or seg not in cur:
            raise PathNotFound()
        cur = cur[seg]
    return cur


def set_path(root, segments, value):
    """按路径赋值，缺失的中间对象自动创建；中间段已存在但不是对象时抛 PathCollision。"""
    cur = root
    for seg in segments[:-1]:
        if seg not in cur:
            cur[seg] = {}
        elif not isinstance(cur[seg], dict):
            raise PathCollision()
        cur = cur[seg]
    cur[segments[-1]] = value


def remove_path(root, segments):
    """删除叶子键，并向上清理因此变空的对象。"""
    get_path(root, segments)  # 不存在则抛 PathNotFound
    chain = [root]
    cur = root
    for seg in segments[:-1]:
        cur = cur[seg]
        chain.append(cur)
    del cur[segments[-1]]
    for parent, seg in zip(reversed(chain[:-1]), reversed(segments[:-1])):
        child = parent.get(seg)
        if isinstance(child, dict) and len(child) == 0:
            del parent[seg]
        else:
            break


# cast 允许的目标类型
CAST_TYPES = ("string", "integer", "number", "boolean")
_INTEGER_RE = __import__("re").compile(r"[+-]?[0-9]+$")
_NUMBER_RE = __import__("re").compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")


def type_name(value):
    """输出签名中使用的类型名。"""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def cast_value(value, target):
    """严格 cast，失败抛 DataValidationError。

    - string：仅接受 string/integer/number/boolean 标量；
    - integer：接受整数本身，或形如 ``12`` / ``-3`` 的字符串；
    - number：接受整数/浮点数，或数值字面量字符串；
    - boolean：接受布尔本身，或字符串 ``true`` / ``false``。
    """
    if target == "string":
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            return value
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            return str(value)
    elif target == "integer":
        if isinstance(value, bool):
            pass
        elif isinstance(value, int):
            return value
        elif isinstance(value, str) and _INTEGER_RE.match(value):
            return int(value)
    elif target == "number":
        if isinstance(value, bool):
            pass
        elif isinstance(value, int):
            return float(value)
        elif isinstance(value, float):
            return value
        elif isinstance(value, str) and _NUMBER_RE.match(value):
            return float(value)
    elif target == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value in ("true", "false"):
            return value == "true"
    raise DataValidationError("无法将值 %r 转换为 %s" % (value, target))
