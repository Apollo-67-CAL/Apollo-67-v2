from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ConnectorSpec:
    id: str
    group: str
    label: str
    status: str
    requires_key: bool
    key_env: Optional[str] = None
    notes: Optional[str] = None


def get_default_connector_registry() -> List[ConnectorSpec]:
    return [
        ConnectorSpec(id="reddit", group="social", label="Reddit", status="live", requires_key=False, notes="RSS/public search feed"),
        ConnectorSpec(id="x", group="social", label="X", status="pending", requires_key=True, key_env="X_BEARER_TOKEN", notes="API integration pending"),
        ConnectorSpec(id="youtube", group="social", label="YouTube", status="pending", requires_key=True, key_env="YOUTUBE_API_KEY", notes="API integration pending"),
        ConnectorSpec(id="tiktok", group="social", label="TikTok", status="stub", requires_key=False, notes="no supported feed yet"),
        ConnectorSpec(id="instagram", group="social", label="Instagram", status="stub", requires_key=False, notes="no supported feed yet"),
        ConnectorSpec(id="facebook", group="social", label="Facebook", status="stub", requires_key=False, notes="no supported feed yet"),
        ConnectorSpec(id="hotcopper", group="social", label="HotCopper", status="stub", requires_key=False, notes="no supported feed yet"),
        ConnectorSpec(id="google_news_rss", group="news", label="Google News RSS", status="live", requires_key=False, notes="RSS search"),
        ConnectorSpec(id="finnhub_news", group="news", label="Finnhub News", status="pending", requires_key=True, key_env="FINNHUB_API_KEY", notes="connector pending"),
        ConnectorSpec(id="gdelt", group="news", label="GDELT", status="live", requires_key=False, notes="doc API"),
        ConnectorSpec(id="sec_edgar", group="news", label="SEC Edgar", status="live", requires_key=False, notes="Atom filings feed"),
        ConnectorSpec(id="placeholder_broker_flows", group="institution", label="Broker Flows", status="stub", requires_key=False, notes="placeholder only"),
    ]


def registry_by_group() -> Dict[str, List[ConnectorSpec]]:
    grouped: Dict[str, List[ConnectorSpec]] = {"social": [], "news": [], "institution": []}
    for spec in get_default_connector_registry():
        grouped.setdefault(spec.group, []).append(spec)
    return grouped
