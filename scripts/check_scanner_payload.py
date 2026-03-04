#!/usr/bin/env python3
import json
import sys
import urllib.request

URL = "http://127.0.0.1:8000/scanner/overall?interval=1day&bars=60&limit=10&refresh=true"


def main() -> int:
    with urllib.request.urlopen(URL, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    rows = payload.get("rows") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        print("rows missing")
        return 1

    violations = []
    for row in rows:
        if not isinstance(row, dict) or row.get("ok") is False:
            continue
        price = row.get("price")
        if price == 0 or price == 0.0:
            violations.append((row.get("symbol"), "price_zero"))
        for key in ("target", "stop", "trail"):
            val = row.get(key)
            if val == 0 or val == 0.0:
                violations.append((row.get("symbol"), f"{key}_zero"))

    if violations:
        print("scanner payload placeholder violations:", violations)
        return 1

    print("scanner payload check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
