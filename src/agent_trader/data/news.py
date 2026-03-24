import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger
from pydantic import BaseModel


class Headline(BaseModel):
    title: str
    seendate: datetime


class NewsArchive(BaseModel):
    by_day: dict[str, list[Headline]]


def _parse_seendate(s: str) -> datetime:
    """Parse GDELT seendate format: YYYYMMDDTHHMMSSZ → datetime UTC."""
    return datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def load_news(path: str | Path) -> NewsArchive:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    by_day: dict[str, list[Headline]] = {}
    total = 0
    for day_key, articles in raw.items():
        headlines = []
        for a in articles:
            title = a.get("title", "").strip()
            seendate_str = a.get("seendate", "")
            if not title or not seendate_str:
                continue
            try:
                seendate = _parse_seendate(seendate_str)
            except ValueError:
                continue
            headlines.append(Headline(title=title, seendate=seendate))
        by_day[day_key] = headlines
        total += len(headlines)

    logger.info(f"Loaded {total} headlines across {len(by_day)} days from {path.name}")
    return NewsArchive(by_day=by_day)


def get_news_context(
    archive: NewsArchive,
    post_datetime: datetime,
    days_back: int = 7,
) -> str:
    cutoff_start = post_datetime - timedelta(days=days_back)

    collected: list[Headline] = []
    current = cutoff_start.date()
    end_date = post_datetime.date()

    while current <= end_date:
        day_key = current.strftime("%Y-%m-%d")
        for h in archive.by_day.get(day_key, []):
            if h.seendate < post_datetime:
                collected.append(h)
        current += timedelta(days=1)

    collected.sort(key=lambda h: h.seendate, reverse=True)
    collected = collected[:50]

    lines = [
        f"[{h.seendate:%Y-%m-%d %H:%M UTC}] {h.title}"
        for h in collected
    ]
    return "\n".join(lines)
