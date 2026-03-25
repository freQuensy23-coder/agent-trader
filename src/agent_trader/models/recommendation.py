import json
from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator


@lru_cache(maxsize=1)
def _load_valid_assets() -> frozenset[str]:
    path = Path("data/proxy_snapshots/allPerpMetas.json")
    if not path.exists():
        return frozenset()
    data = json.loads(path.read_text())
    assets = set()
    for group in data:
        for u in group.get("universe", []):
            assets.add(u["name"])
    return frozenset(assets)


class AssetPrediction(BaseModel):
    asset: str
    direction: Literal["up", "down"]
    timeframe: Literal["5m", "15m", "30m", "1h", "4h"]
    confidence: Literal["high", "very_high"]

    @model_validator(mode="after")
    def validate_asset(self) -> Self:
        valid = _load_valid_assets()
        if valid and self.asset not in valid:
            raise ValueError(f"Unknown asset: {self.asset}. Must be a valid HyperLiquid perp ticker.")
        return self


class Recommendation(BaseModel):
    action: Literal["signal", "skip"]
    reasoning: str = Field(min_length=20)
    importance_score: int = Field(ge=1, le=10)
    predictions: list[AssetPrediction] = Field(default_factory=list, max_length=3)
    market_analysis: str = Field(default="")

    @model_validator(mode="after")
    def validate_consistency(self) -> Self:
        if self.action == "skip" and self.predictions:
            raise ValueError("action='skip' must have empty predictions")
        if self.action == "signal" and not self.predictions:
            raise ValueError("action='signal' must have at least one prediction")
        if self.importance_score < 7 and self.action == "signal":
            raise ValueError("importance_score < 7 requires action='skip'")
        return self
