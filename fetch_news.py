#!/usr/bin/env python3
"""Lightweight RSS/Atom aggregator -> dist/feed.json"""
import hashlib
import html
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser
import requests
import yaml

ROOT = Path(__file__).parent
SOURCES_FILE = ROOT / "sources.yaml"
STATE_FILE = ROOT / "cache" / "etag_state.json"
ARTICLES_CACHE = ROOT / "cache" / "last_articles.json"
OUT_FILE = ROOT / "dist" / "feed.json"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
}
TIMEOUT = 8
MAX_WORKERS = 6
TRACKING = re.compile(r"^(utm_.*|fbclid|ref)$", re.I)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def warn(msg):
    print(f"WARN: {msg}", file=sys.stderr)


def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def normalize_url(url):
    p = urlsplit(url.strip())
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not TRACKING.match(k)]
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q), ""))


def plain_text(raw, limit=280):
    text = WS_RE.sub(" ", html.unescape(TAG_RE.sub(" ", raw or ""))).strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def thumbnail(entry):
    for key in ("media_thumbnail", "media_content"):
        for m in entry.get(key) or []:
            if m.get("url"):
                return m["url"]
    for enc in entry.get("enclosures") or []:
        if enc.get("url") and str(enc.get("type", "image")).startswith("image"):
            return enc["url"]
    for link in entry.get("links") or []:
        if link.get("rel") == "enclosure" and str(link.get("type", "")).startswith("image"):
            return link.get("href")
    return None


def published_iso(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    dt = datetime(*t[:6], tzinfo=timezone.utc) if t else datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_entries(content, src, limit):
    feed = feedparser.parse(content)
    items = []
    for e in feed.entries[:limit]:
        link, title = e.get("link"), plain_text(e.get("title", ""), 300)
        if not link or not title:
            continue
        url = normalize_url(link)
        items.append({
            "id": hashlib.sha256(url.encode()).hexdigest()[:16],
            "title": title,
            "url": url,
            "source": src["name"],
            "category": src["category"],
            "published_at": published_iso(e),
            "summary": plain_text(e.get("summary") or e.get("description") or ""),
            "image_url": thumbnail(e),
        })
    return items


def fetch_source(src, state, cache, limit):
    """Returns (source_id, items, new_state_entry)."""
    url = src["url"]
    st = dict(state.get(url) or {})
    st.setdefault("fail_count", 0)
    headers = dict(HEADERS)
    if st.get("etag"):
        headers["If-None-Match"] = st["etag"]
    if st.get("last_modified"):
        headers["If-Modified-Since"] = st["last_modified"]
    cached = cache.get(src["id"], [])
    try:
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 304:
            st["fail_count"] = 0
            return src["id"], cached, st
        r.raise_for_status()
        items = parse_entries(r.content, src, limit)
        st.update(etag=r.headers.get("ETag"), last_modified=r.headers.get("Last-Modified"), fail_count=0)
        return src["id"], items, st
    except Exception as exc:  # never crash the run
        st["fail_count"] += 1
        warn(f"{src['id']} failed ({type(exc).__name__}: {str(exc)[:100]}); using cached items")
        return src["id"], cached, st


def matches(text, kws):
    return any(k.lower() in text for k in kws or [])


def keep(item, cfg):
    cat = (cfg.get("categories") or {}).get(item["category"]) or {}
    text = f"{item['title']} {item['summary']}".lower()
    if matches(text, (cfg.get("exclude_keywords") or []) + (cat.get("exclude_keywords") or [])):
        return False
    inc = (cfg.get("include_keywords") or []) + (cat.get("include_keywords") or [])
    return matches(text, inc) if inc else True


def main():
    cfg = yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8")) or {}
    per_source = int(cfg.get("max_articles_per_source", 25))
    total = int(cfg.get("max_total_articles", 250))
    sources = [s for s in cfg.get("sources", []) if s.get("enabled", True)]
    state = load_json(STATE_FILE, {})
    cache = load_json(ARTICLES_CACHE, {})

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(lambda s: fetch_source(s, state, cache, per_source), sources))

    new_cache, seen, articles = {}, set(), []
    for src, (sid, items, st) in zip(sources, results):
        state[src["url"]] = st
        new_cache[sid] = items
        for it in items:
            if it["id"] not in seen and keep(it, cfg):
                seen.add(it["id"])
                articles.append(it)

    articles.sort(key=lambda a: a["published_at"], reverse=True)
    articles = articles[:total]
    out = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "article_count": len(articles),
        "articles": articles,
    }
    save_json(OUT_FILE, out)
    save_json(STATE_FILE, state)
    save_json(ARTICLES_CACHE, new_cache)
    print(f"Wrote {len(articles)} articles to {OUT_FILE}")


if __name__ == "__main__":
    main()
