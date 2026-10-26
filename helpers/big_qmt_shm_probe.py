"""External Windows diagnostics; never sends orders or cancels.

Run with the same --env-file as bullet-trade server. With --pending, only inspect
the external journal. --resolve requires prior manual reconciliation in QMT.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--pending", action="store_true")
    parser.add_argument("--resolve", metavar="REQUEST_ID")
    parser.add_argument("--note", help="Operator reconciliation evidence; required with --resolve")
    args = parser.parse_args()
    from bullet_trade.utils.env_loader import load_env
    load_env(args.env_file, override=True)
    from bullet_trade.server.config import ServerConfig
    from bullet_trade.server.adapters.big_qmt import BigQmtGatewayClient, load_big_qmt_gateway_config
    from bullet_trade.server.big_qmt_shm import ShmJournal
    config = load_big_qmt_gateway_config(ServerConfig())
    if config.transport != "shm":
        parser.error("BIG_QMT_GATEWAY_TRANSPORT must be shm")
    if args.pending or args.resolve:
        if not Path(config.shm_journal).expanduser().is_file():
            parser.error("Journal does not exist; use the same working directory/path as the server")
        journal = ShmJournal(config.shm_journal, config.shm_name)
        db = journal.connect()
        try:
            if args.resolve:
                if not args.note:
                    parser.error("--resolve requires --note with the completed manual reconciliation")
                # Do not delete the original intent or permit its idempotency key to replay.
                resolved = {"ok": False, "code": "IPC_MANUALLY_RECONCILED",
                            "message": "Operator reconciled outcome: " + args.note}
                with db:
                    cursor = db.execute("UPDATE intents SET response=? WHERE channel=? AND request_id=? AND response IS NULL",
                                        (json.dumps(resolved, ensure_ascii=False), config.shm_name, args.resolve))
                    if cursor.rowcount != 1:
                        raise RuntimeError("Expected exactly one pending intent; no matching request")
            rows = db.execute("SELECT request_id,created,details FROM intents WHERE channel=? AND response IS NULL ORDER BY created", (config.shm_name,)).fetchall()
            print(json.dumps([{"request_id": r[0], "created": r[1], "details": json.loads(r[2])} for r in rows], ensure_ascii=False, indent=2))
        finally:
            db.close()
        return
    client = BigQmtGatewayClient(config)
    health = client.request_json("/health", None, "GET")
    # Print only operational diagnostics, not credentials or account balances.
    fields = ("ready", "transport", "shm_alive", "shm_name", "shm_size", "shm_session",
              "ctypes_load_mode", "gateway_build_id", "context_ready", "trading_enabled")
    print(json.dumps({key: health.get(key) for key in fields}, ensure_ascii=False, indent=2))
    if not health.get("ready") or not health.get("shm_alive"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
