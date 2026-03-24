import json
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import pandas as pd
from loguru import logger

from agent_trader.models.post import TruthPost


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip = True
        elif tag in ("br", "p", "div"):
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts).strip()


def _strip_html(html: str) -> str:
    stripper = _HTMLStripper()
    stripper.feed(html)
    return stripper.get_text()


def _parse_datetime(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_post_json(raw: dict) -> TruthPost:
    post_id = str(raw["id"])
    created_at = _parse_datetime(raw["created_at"])
    created_at_ms = int(created_at.timestamp() * 1000)

    content = raw.get("content", "") or ""
    raw_html = content
    media = raw.get("media_attachments") or []
    reblog = raw.get("reblog")
    text = _strip_html(content)

    if not text and reblog:
        is_repost = True
        text = "[Repost]"
    elif not text and media:
        is_repost = False
        text = "[Media only]"
    elif not text:
        is_repost = bool(reblog)
        text = "[Repost]" if is_repost else ""
    else:
        is_repost = bool(reblog)

    reblogs = raw.get("reblogs_count") or 0
    favourites = raw.get("favourites_count") or 0
    replies = raw.get("replies_count") or 0
    engagement = int(reblogs) + int(favourites) + int(replies)

    return TruthPost(
        id=post_id,
        created_at=created_at,
        created_at_ms=created_at_ms,
        text=text,
        url=raw.get("url", "") or "",
        engagement=engagement,
        has_media=bool(media),
        is_repost=is_repost,
        raw_html=raw_html,
    )


def _parse_post_parquet(row: pd.Series) -> TruthPost:
    post_id = str(row["id"])
    created_at = _parse_datetime(str(row["created_at"]))
    created_at_ms = int(created_at.timestamp() * 1000)

    content_text = str(row.get("content_text", "") or "")
    raw_html = str(row.get("content_html", "") or "")
    is_repost = bool(row.get("is_reblog", False))

    media_urls_str = str(row.get("media_urls", "[]") or "[]")
    try:
        media_urls = json.loads(media_urls_str)
    except (json.JSONDecodeError, TypeError):
        media_urls = []
    has_media = bool(media_urls)

    text = content_text.strip()
    if not text and is_repost:
        text = "[Repost]"
    elif not text and has_media:
        text = "[Media only]"

    reblogs = int(row.get("reblogs_count", 0) or 0)
    favourites = int(row.get("favourites_count", 0) or 0)
    replies = int(row.get("replies_count", 0) or 0)

    return TruthPost(
        id=post_id,
        created_at=created_at,
        created_at_ms=created_at_ms,
        text=text,
        url=str(row.get("url", "") or ""),
        engagement=reblogs + favourites + replies,
        has_media=has_media,
        is_repost=is_repost,
        raw_html=raw_html,
    )


def _load_parquet(path: Path) -> list[dict]:
    df = pd.read_parquet(path)
    raw_posts = []
    for _, row in df.iterrows():
        raw_posts.append(("parquet", row))
    return raw_posts


def load_posts(
    path: str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[TruthPost]:
    path = Path(path)

    posts: dict[str, TruthPost] = {}
    total_raw = 0

    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
        total_raw = len(df)
        for _, row in df.iterrows():
            try:
                post = _parse_post_parquet(row)
            except Exception as e:
                logger.warning(f"Skipping post: {e}")
                continue
            if post.id not in posts:
                posts[post.id] = post
    else:
        with open(path, "r", encoding="utf-8") as f:
            raw_list = json.load(f)
        total_raw = len(raw_list)
        for raw in raw_list:
            try:
                post = _parse_post_json(raw)
            except Exception as e:
                logger.warning(f"Skipping post: {e}")
                continue
            if post.id not in posts:
                posts[post.id] = post

    result = sorted(posts.values(), key=lambda p: p.created_at)

    if start:
        result = [p for p in result if p.created_at >= start]
    if end:
        result = [p for p in result if p.created_at <= end]

    logger.info(f"Loaded {len(result)} posts from {path.name} (deduped from {total_raw} raw)")
    if result:
        logger.info(f"  Date range: {result[0].created_at:%Y-%m-%d} — {result[-1].created_at:%Y-%m-%d}")
    return result
