from __future__ import annotations

import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.scanners.connectors.registry import ConnectorSpec, get_default_connector_registry


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_get(url: str, timeout: int = 10) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Apollo67/1.0 scanner-pipeline",
            "Accept": "application/json, application/rss+xml, application/xml, text/xml, */*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def _parse_rss(xml_text: str, limit: int = 20) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if not xml_text:
        return items
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return items

    channel = root.find("channel")
    if channel is not None:
        for item in channel.findall("item")[:limit]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            summary = (item.findtext("description") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            items.append({"title": title, "url": link, "text": f"{title} {summary}".strip(), "ts": pub_date})
        return items

    ns = {"a": "http://www.w3.org/2005/Atom"}
    for entry in root.findall("a:entry", ns)[:limit]:
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        summary = (entry.findtext("a:summary", default="", namespaces=ns) or "").strip()
        updated = (entry.findtext("a:updated", default="", namespaces=ns) or "").strip()
        link_el = entry.find("a:link", ns)
        url = (link_el.get("href") if link_el is not None else "") or ""
        items.append({"title": title, "url": url, "text": f"{title} {summary}".strip(), "ts": updated})
    return items


def _fetch_reddit(symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    query = urllib.parse.quote_plus(symbol)
    url = f"https://www.reddit.com/search.rss?q={query}&sort=new"
    return _parse_rss(_http_get(url), limit=limit)


def _fetch_google_news(symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    query = urllib.parse.quote_plus(f"{symbol} stock when:7d")
    url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    return _parse_rss(_http_get(url), limit=limit)


def _fetch_gdelt(symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    query = urllib.parse.quote_plus(f"{symbol} stock")
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=ArtList&maxrecords={max(1, min(limit, 50))}&format=json"
    raw = _http_get(url)
    data = json.loads(raw)
    articles = data.get("articles") if isinstance(data, dict) else []
    items: List[Dict[str, Any]] = []
    if isinstance(articles, list):
        for article in articles[:limit]:
            if not isinstance(article, dict):
                continue
            title = str(article.get("title") or "").strip()
            url_v = str(article.get("url") or "").strip()
            text = str(article.get("seendate") or "")
            items.append({"title": title, "url": url_v, "text": title, "ts": text})
    return items


def _fetch_sec_edgar(symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    if not symbol.isalpha() or len(symbol) > 5:
        return []
    query = urllib.parse.quote_plus(symbol)
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={query}&owner=exclude&count={max(1, min(limit, 40))}&output=atom"
    return _parse_rss(_http_get(url), limit=limit)


def fetch_items(symbol: str, group: str, connectors_enabled: Dict[str, bool]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    symbol_value = (symbol or "").strip().upper()
    group_value = (group or "social").strip().lower()
    items: List[Dict[str, Any]] = []
    runtime: Dict[str, Dict[str, Any]] = {}

    registry = [spec for spec in get_default_connector_registry() if spec.group == group_value]

    for spec in registry:
        enabled = bool(connectors_enabled.get(spec.id, False))
        status = spec.status
        runtime_item: Dict[str, Any] = {
            "last_run": None,
            "last_error": None,
            "status": status,
            "enabled": enabled,
        }
        runtime[spec.id] = runtime_item

        if not enabled or status in {"stub", "pending", "disabled"}:
            continue

        try:
            fetched: List[Dict[str, Any]] = []
            if spec.id == "reddit":
                fetched = _fetch_reddit(symbol_value)
            elif spec.id == "google_news_rss":
                fetched = _fetch_google_news(symbol_value)
            elif spec.id == "gdelt":
                fetched = _fetch_gdelt(symbol_value)
            elif spec.id == "sec_edgar":
                fetched = _fetch_sec_edgar(symbol_value)

            for item in fetched:
                items.append(
                    {
                        "source_id": spec.id,
                        "source_label": spec.label,
                        "url": item.get("url"),
                        "title": item.get("title"),
                        "text": item.get("text") or item.get("title") or "",
                        "ts": item.get("ts") or _now_iso(),
                    }
                )
            runtime_item["last_run"] = _now_iso()
        except Exception as exc:
            runtime_item["last_error"] = str(exc)
            runtime_item["last_run"] = _now_iso()

    return items, runtime
