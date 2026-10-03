"""命令行入口：``stream-etl run`` / ``stream-etl replay``。

用法::

    stream-etl run    --config CONFIG --output OUTPUT --checkpoint CHECKPOINT
    stream-etl replay --config CONFIG --output OUTPUT --checkpoint CHECKPOINT
"""

import sys

from .config import load_config
from .engine import Engine
from .errors import ConfigurationError, StreamETLError

_USAGE = (
    "用法:\n"
    "  stream-etl run    --config <配置> --output <输出.jsonl> --checkpoint <检查点>\n"
    "  stream-etl replay --config <配置> --output <输出.jsonl> --checkpoint <检查点>\n"
)

_COMMANDS = ("run", "replay")


def _parse_args(argv):
    if len(argv) < 1 or argv[0] not in _COMMANDS:
        raise ConfigurationError(
            "未知或缺少子命令（仅支持 %s）" % "、".join(_COMMANDS)
        )
    command = argv[0]
    values = {}
    i = 1
    while i < len(argv):
        token = argv[i]
        if not token.startswith("--") or "=" in token:
            raise ConfigurationError("非法参数: %s" % token)
        key = token[2:]
        if key not in ("config", "output", "checkpoint"):
            raise ConfigurationError("未知参数: %s" % token)
        i += 1
        if i >= len(argv):
            raise ConfigurationError("参数 %s 缺少取值" % token)
        if key in values:
            raise ConfigurationError("参数 --%s 重复" % key)
        values[key] = argv[i]
        i += 1
    missing = [k for k in ("config", "output", "checkpoint") if k not in values]
    if missing:
        raise ConfigurationError("缺少参数: %s" % ", ".join("--" + m for m in missing))
    for key, value in values.items():
        if value == "":
            raise ConfigurationError("--%s 不能为空" % key)
    return command, values["config"], values["output"], values["checkpoint"]


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    try:
        command, config_path, output_path, checkpoint_path = _parse_args(argv)
        config = load_config(config_path)
        engine = Engine(config, output_path, checkpoint_path)
        if command == "run":
            engine.run()
        else:
            engine.replay()
    except StreamETLError as exc:
        sys.stderr.write(exc.render() + "\n")
        return exc.exit_code
    except BrokenPipeError:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
