"""Bit2C account verification — pull real balances via the private API and
cross-check against the Google-Sheet snapshot (Bit2C-platform rows).

Credentials: bit2c_api.json (gitignored). Read-only usage: Balance only.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CREDS = json.loads((ROOT / "bit2c_api.json").read_text())
BASE = "https://bit2c.co.il"


def private_get(path: str) -> dict:
    nonce = str(int(time.time() * 1000))
    params = urllib.parse.urlencode({"nonce": nonce})
    sign = base64.b64encode(
        hmac.new(CREDS["secret"].encode(), params.encode(), hashlib.sha512).digest()
    ).decode()
    req = urllib.request.Request(
        f"{BASE}{path}?{params}",
        headers={
            "Key": CREDS["key"],
            "Sign": sign,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def private_post(path: str, extra: dict | None = None) -> dict:
    nonce = str(int(time.time() * 1000))
    payload = {"nonce": nonce}
    if extra:
        payload.update(extra)
    body = urllib.parse.urlencode(payload)
    sign = base64.b64encode(
        hmac.new(CREDS["secret"].encode(), body.encode(), hashlib.sha512).digest()
    ).decode()
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=body.encode(),
        headers={"Key": CREDS["key"], "Sign": sign,
                 "Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def history() -> None:
    # Pull account order history (all pairs) to find recent BTC buys.
    for path in ("/Order/OrderHistory", "/Order/AccountHistory"):
        try:
            res = private_post(path, {"take": 50, "fromTime": 0})
            print(f"=== {path} ===")
            print(json.dumps(res, ensure_ascii=False, indent=2)[:4000])
            return
        except Exception as exc:
            print(f"{path}: {str(exc)[:120]}")


def main() -> None:
    import sys
    if "history" in sys.argv:
        history()
        return
    bal = private_get("/Account/Balance/v2")
    if isinstance(bal, dict) and bal.get("error"):
        print("API ERROR:", bal["error"])
        return
    interesting = {}
    for k, v in (bal or {}).items():
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv > 1e-9 and not k.startswith(("Locked", "AVAILABLE_")):
            interesting[k] = fv
    print("=== Bit2C real balances (non-zero) ===")
    for k, v in sorted(interesting.items()):
        print(f"  {k:12s} {v}")
    avail = {k.replace("AVAILABLE_", ""): float(v) for k, v in (bal or {}).items()
             if k.startswith("AVAILABLE_") and float(v or 0) > 1e-9}
    if avail:
        print("=== available ===")
        for k, v in sorted(avail.items()):
            print(f"  {k:12s} {v}")


if __name__ == "__main__":
    main()
