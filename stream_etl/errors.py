"""错误类型与退出码。

错误结果确定可观察：stderr 输出 ``Error: <错误类型>``，退出码固定。
"""

# 退出码
EXIT_OK = 0
EXIT_CONFIG = 2
EXIT_DATA = 3
EXIT_CHECKPOINT = 4
EXIT_IO = 5


class StreamETLError(Exception):
    """所有受控错误的基类，携带退出码。"""

    exit_code = EXIT_IO
    label = "StreamETLError"

    def __init__(self, message=""):
        super().__init__(message)
        self.message = message

    def render(self):
        """渲染为 stderr 上的一行 ``Error: <类型>: 详情``。"""
        if self.message:
            return "Error: %s: %s" % (self.label, self.message)
        return "Error: %s" % self.label


class ConfigurationError(StreamETLError):
    """配置失败：source 不完整、操作未知、路径非法、参数非法。"""

    exit_code = EXIT_CONFIG
    label = "ConfigurationError"


class DataValidationError(StreamETLError):
    """记录缺少必要字段、字段路径不存在、cast 失败。"""

    exit_code = EXIT_DATA
    label = "DataValidationError"


class CheckpointError(StreamETLError):
    """检查点损坏、版本不匹配、无法恢复。"""

    exit_code = EXIT_CHECKPOINT
    label = "CheckpointError"


class SourceError(StreamETLError):
    """输入不可读。"""

    exit_code = EXIT_IO
    label = "SourceError"


class SinkError(StreamETLError):
    """输出不可写。"""

    exit_code = EXIT_IO
    label = "SinkError"
