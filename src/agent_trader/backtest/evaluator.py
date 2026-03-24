import httpx
from loguru import logger

from agent_trader.data.market import fetch_price_change
from agent_trader.models.outcome import PredictionOutcome
from agent_trader.models.post import TruthPost
from agent_trader.models.recommendation import Recommendation


class Evaluator:
    async def evaluate(
        self,
        rec: Recommendation,
        post: TruthPost,
        client: httpx.AsyncClient | None = None,
    ) -> list[PredictionOutcome]:
        outcomes: list[PredictionOutcome] = []

        for pred in rec.predictions:
            try:
                price_at, price_after, change_pct = await fetch_price_change(
                    pred.asset, post.created_at_ms, pred.timeframe, client
                )
            except Exception as e:
                logger.warning(f"No price data for {pred.asset} @ {post.created_at_ms}: {e}")
                continue

            direction_correct = (
                (pred.direction == "up" and change_pct > 0)
                or (pred.direction == "down" and change_pct < 0)
            )

            outcomes.append(PredictionOutcome(
                prediction=pred,
                actual_price_at_post=price_at,
                actual_price_after=price_after,
                actual_change_pct=change_pct,
                direction_correct=direction_correct,
                post_id=post.id,
                post_text=post.text,
            ))

        return outcomes
