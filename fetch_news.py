#!/usr/bin/env python3
"""Lightweight RSS/Atom aggregator + HTML scraper -> dist/feed.json

Sources are RSS/Atom by default. `type: html` sources are scraped: links on the listing page
that match `link_pattern` are articles, and each new article page is fetched once for its
og:/JSON-LD metadata (title, summary, image, publish time), which is cached in SCRAPE_CACHE.
"""
import hashlib
import html
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import requests
import yaml

ROOT = Path(__file__).parent
SOURCES_FILE = ROOT / "sources.yaml"
STATE_FILE = ROOT / "cache" / "etag_state.json"
ARTICLES_CACHE = ROOT / "cache" / "last_articles.json"
SCRAPE_CACHE = ROOT / "cache" / "scrape_meta.json"
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


# ------------------------------------------------------------------------- HTML scraping

class _PageParser(HTMLParser):
    """Collects <a href> links with their text, and <meta> property/name -> content."""

    def __init__(self, base):
        super().__init__(convert_charrefs=True)
        self.base, self.links, self.meta, self._open = base, [], {}, []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "a" and a.get("href"):
            self._open.append([urljoin(self.base, a["href"]), a.get("title") or ""])
        elif tag == "meta" and a.get("content"):
            key = (a.get("property") or a.get("name") or "").lower()
            if key and key not in self.meta:
                self.meta[key] = a["content"]

    def handle_data(self, data):
        for link in self._open:
            link[1] += " " + data

    def handle_endtag(self, tag):
        if tag == "a" and self._open:
            href, text = self._open.pop()
            self.links.append((href, WS_RE.sub(" ", text).strip()))


# Embedded JSON (JSON-LD, Next.js/Contentful props): first publish-like key wins.
EMBEDDED_DATE_RES = [re.compile(rf'"{k}"\s*:\s*"([^"]+)"')
                     for k in ("datePublished", "publishDate", "publishedAt", "published_at", "dateModified")]
TITLE_SEPS = (" | ", " - ", " – ", " — ", ": ")


def strip_site_name(title, site):
    """'Site | Headline' / 'Headline - Site' -> 'Headline'."""
    if site:
        for sep in TITLE_SEPS:
            if title.startswith(site + sep):
                return title[len(site + sep):]
            if title.endswith(sep + site):
                return title[: -len(sep + site)]
    return title


def _decode(r):
    if r.encoding is None or r.encoding.lower() == "iso-8859-1":  # requests' default for text/html
        r.encoding = r.apparent_encoding
    return r.text


def _iso(value):
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def article_meta(url, fallback_title):
    """Fetches one article page and reads its og:/twitter:/JSON-LD metadata."""
    meta = {"title": fallback_title, "summary": "", "image_url": None, "published_at": None}
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        page = _decode(r)
    except Exception as exc:
        warn(f"article {url[:80]} failed ({type(exc).__name__}); using link text")
        return meta
    p = _PageParser(url)
    p.feed(page)
    m = p.meta
    title = plain_text(m.get("og:title") or m.get("twitter:title") or fallback_title, 300)
    meta["title"] = strip_site_name(title, m.get("og:site_name") or m.get("application-name")) or title
    meta["summary"] = plain_text(m.get("og:description") or m.get("description") or "")
    meta["image_url"] = m.get("og:image") or m.get("twitter:image")
    date = m.get("article:published_time") or m.get("og:published_time")
    dates = [date] + [mt.group(1) for rx in EMBEDDED_DATE_RES if (mt := rx.search(page))]
    meta["published_at"] = next((iso for d in dates if d and (iso := _iso(d))), None)
    return meta


def scrape_entries(r, src, limit, scrape_cache):
    """Article links on a listing page -> items. Only pages not in scrape_cache are fetched."""
    p = _PageParser(r.url)
    p.feed(_decode(r))
    pattern = re.compile(src["link_pattern"])
    min_len = int(src.get("min_title_length", 0))
    strip = re.compile(src["title_strip"]) if src.get("title_strip") else None
    links = {}  # url -> longest link text (cards often repeat a link as image + "Watch now")
    for href, text in p.links:
        url = normalize_url(href)
        if pattern.search(url) and len(text) >= len(links.get(url, "")):
            links[url] = text
    links = {u: t for u, t in links.items() if len(t) >= min_len}  # drops nav links like "Key dates"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = []
    for url, text in list(links.items())[:limit]:
        meta = scrape_cache.get(url)
        if meta is None:
            meta = article_meta(url, plain_text(text, 300) or url.rsplit("/", 1)[-1].replace("-", " "))
            meta["first_seen"] = now
            scrape_cache[url] = meta
        items.append({
            "id": hashlib.sha256(url.encode()).hexdigest()[:16],
            "title": (strip.sub("", meta["title"]).strip() if strip else "") or meta["title"],
            "url": url,
            "source": src["name"],
            "category": src["category"],
            # No publish time on the page: use when we first saw it, so it doesn't look new every run.
            "published_at": meta.get("published_at") or meta["first_seen"],
            "summary": meta["summary"],
            "image_url": meta["image_url"],
        })
    if not items:
        raise ValueError(f"no links matched {src['link_pattern']!r}")  # layout change: keep cache
    return items


# ------------------------------------------------------------------------- fetching

def fetch_source(src, state, cache, limit, scrape_cache):
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
        if src.get("type", "rss") == "html":
            items = scrape_entries(r, src, limit, scrape_cache)
        else:
            items = parse_entries(r.content, src, limit)
        st.update(etag=r.headers.get("ETag"), last_modified=r.headers.get("Last-Modified"), fail_count=0)
        return src["id"], items, st
    except Exception as exc:  # never crash the run
        st["fail_count"] += 1
        warn(f"{src['id']} failed ({type(exc).__name__}: {str(exc)[:100]}); using cached items")
        return src["id"], cached, st


def matches(text, kws):
    """Whole-word, case-insensitive match, so "oda" doesn't hit "today"."""
    return any(re.search(rf"\b{re.escape(k.lower())}\b", text) for k in kws or [])


def keep(item, src, cfg):
    """Applies global, category and per-source keyword filters."""
    cat = (cfg.get("categories") or {}).get(item["category"]) or {}
    scopes = (cfg, cat, src)
    text = f"{item['title']} {item['summary']}".lower()
    if matches(text, [k for s in scopes for k in s.get("exclude_keywords") or []]):
        return False
    inc = [k for s in scopes for k in s.get("include_keywords") or []]
    return matches(text, inc) if inc else True


def main():
    cfg = yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8")) or {}
    per_source = int(cfg.get("max_articles_per_source", 25))
    total = int(cfg.get("max_total_articles", 250))
    sources = [s for s in cfg.get("sources", []) if s.get("enabled", True)]
    state = load_json(STATE_FILE, {})
    cache = load_json(ARTICLES_CACHE, {})
    scrape_cache = load_json(SCRAPE_CACHE, {})

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(
            lambda s: fetch_source(s, state, cache, int(s.get("max_articles", per_source)), scrape_cache),
            sources))

    new_cache, seen, articles = {}, set(), []
    for src, (sid, items, st) in zip(sources, results):
        state[src["url"]] = st
        new_cache[sid] = items
        for it in items:
            if it["id"] not in seen and keep(it, src, cfg):
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
    # Keep metadata only for article URLs that are still listed, so the cache can't grow forever.
    listed = {it["url"] for items in new_cache.values() for it in items}
    save_json(SCRAPE_CACHE, {u: m for u, m in scrape_cache.items() if u in listed})
    print(f"Wrote {len(articles)} articles to {OUT_FILE}")


if __name__ == "__main__":
    main()
