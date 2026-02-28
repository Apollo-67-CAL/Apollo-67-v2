import json
import logging
from collections import defaultdict
from typing import Dict

logger = logging.getLogger("apollo67.ingestion")


class IngestionMetrics:
    def __init__(self) -> None:
        self._counters: Dict[str, int] = defaultdict(int)

    def incr(self, name: str, value: int = 1) -> None:
        self._counters[name] += value

    def snapshot(self) -> Dict[str, int]:
        return dict(self._counters)


METRICS = IngestionMetrics()


def log_event(event: str, **fields: object) -> None:
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, sort_keys=True, default=str))
