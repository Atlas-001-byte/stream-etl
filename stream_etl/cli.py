"""Command line interface: ``stream-etl run`` / ``stream-etl replay``.

Usage::

    stream-etl run    --config CONFIG --output OUT --checkpoint CKPT
    stream-etl replay --config CONFIG --output OUT --checkpoint CKPT

On success nothing is written to stdout and the exit code is 0. Any domain
error is printed to stderr as ``Error: <Type>[: detail]`` with its fixed
exit code.
"""

import argparse
import sys

from .config import load_config
from .engine import replay, run
from .errors import StreamETLError


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="stream-etl",
        description="Streaming ETL with schema evolution and replay.",
    )
    sub = parser.add_subparsers(dest="command")
    sub.required = True

    def add_common(p):
        p.add_argument("--config", "-c", required=True,
                       help="path to the YAML configuration")
        p.add_argument("--output", "-o", required=True,
                       help="path to the JSON Lines output file")
        p.add_argument("--checkpoint", "-p", required=True,
                       help="path to the checkpoint file")

    p_run = sub.add_parser("run", help="fresh run with new output/checkpoint")
    add_common(p_run)
    p_replay = sub.add_parser(
        "replay", help="resume from the last committed batch"
    )
    add_common(p_replay)
    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "run":
            run(config, args.output, args.checkpoint)
        else:
            replay(config, args.output, args.checkpoint)
    except StreamETLError as exc:
        print(exc.render(), file=sys.stderr)
        return exc.exit_code
    return 0


if __name__ == "__main__":
    sys.exit(main())
