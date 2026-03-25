from dataclasses import dataclass, field
from datetime import datetime

from agent_trader.data.market import Candle
from agent_trader.models.post import TruthPost
from agent_trader.models.recommendation import AssetPrediction, Recommendation


@dataclass(frozen=True)
class PredictionOutcome:
    prediction: AssetPrediction
    actual_price_at_post: float
    actual_price_after: float
    actual_change_pct: float
    direction_correct: bool
    post_id: str
    post_text: str
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class PostResult:
    post: TruthPost
    recommendation: Recommendation
    outcomes: list[PredictionOutcome]
    agent_cost_usd: float
    agent_turns: int
    chart_candles: dict[str, list[Candle]]


@dataclass
class BacktestResult:
    run_id: str
    posts_total: int
    posts_with_signal: int
    posts_skipped: int
    results: list[PostResult]
    start_date: datetime
    end_date: datetime
    total_agent_cost: float
