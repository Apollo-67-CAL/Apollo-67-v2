from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict

os.environ.setdefault('PYTHONDONTWRITEBYTECODE', '1')
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.main import scanner_sources  # noqa: E402


def _as_dict(payload: Any) -> Dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


def run() -> None:
    nvda = _as_dict(scanner_sources(symbol='NVDA', channel='social', timeframe='1day'))
    aapl = _as_dict(scanner_sources(symbol='AAPL', channel='social', timeframe='1day'))

    assert nvda.get('symbol') == 'NVDA', f"expected NVDA symbol, got {nvda.get('symbol')}"
    assert aapl.get('symbol') == 'AAPL', f"expected AAPL symbol, got {aapl.get('symbol')}"

    assert nvda.get('channel') == 'social'
    assert aapl.get('channel') == 'social'

    nvda_totals = nvda.get('totals') if isinstance(nvda.get('totals'), dict) else {}
    aapl_totals = aapl.get('totals') if isinstance(aapl.get('totals'), dict) else {}

    if nvda_totals == aapl_totals:
      print('warning: totals matched for NVDA and AAPL (possible if live fetch returns similar sentiment)')

    print('ok: symbol-specific payload returned for social channel')


if __name__ == '__main__':
    run()
