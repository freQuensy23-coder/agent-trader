from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TruthPost:
    id: str
    created_at: datetime
    created_at_ms: int
    text: str
    url: str
    engagement: int
    has_media: bool
    is_repost: bool
    raw_html: str
