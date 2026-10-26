"""Embed the Python 3.6 IPC module into the single-file QMT strategy.

Run this in external Python after editing shared_memory_rpc.py; --check is for CI.
"""
import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = (root / "bullet_trade/server/shared_memory_rpc.py").read_text(encoding="utf-8").rstrip()
    helper = root / "helpers/big_qmt_gateway_strategy_sample.py"
    text = helper.read_text(encoding="gbk")
    start, end = "# BEGIN GENERATED SHARED MEMORY RPC\n", "# END GENERATED SHARED MEMORY RPC\n"
    block = start + source + "\n" + end
    if start in text:
        before, rest = text.split(start, 1)
        _, after = rest.split(end, 1)
        updated = before + block + after
    else:
        updated = text.replace("def _boot_print(", block + "\n\ndef _boot_print(", 1)
    if args.check:
        if updated != text:
            raise SystemExit("QMT embedded IPC is stale; run helpers/sync_big_qmt_ipc.py")
    else:
        helper.write_text(updated, encoding="gbk")


if __name__ == "__main__":
    main()
