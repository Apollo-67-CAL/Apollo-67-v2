from __future__ import annotations

import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Dict, List


def _http_get(url: str, timeout: int = 12) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Apollo67/1.0 (+https://localhost) RSS",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def parse_rss(xml_text: str, limit: int = 25) -> List[Dict[str, str]]:
    items = []
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
            items.append({"title": title, "link": link, "summary": summary})
        return items

    ns = {"a": "http://www.w3.org/2005/Atom"}
    for entry in root.findall("a:entry", ns)[:limit]:
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        link_el = entry.find("a:link", ns)
        link = (link_el.get("href") if link_el is not None else "") or ""
        summary = (entry.findtext("a:summary", default="", namespaces=ns) or "").strip()
        items.append({"title": title, "link": link, "summary": summary})
    return items


def fetch_rss(url: str, limit: int = 25) -> List[Dict[str, str]]:
    return parse_rss(_http_get(url), limit=limit)


def q(text: str) -> str:
    return urllib.parse.quote_plus((text or "").strip())
