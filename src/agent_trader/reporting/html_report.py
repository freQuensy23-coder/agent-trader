import json
from pathlib import Path

import jinja2
from rich.console import Console
from rich.table import Table

from agent_trader.models.outcome import BacktestResult

TEMPLATE_PATH = Path(__file__).parent / "template.html"


class HtmlReport:
    def __init__(self, result: BacktestResult):
        self.result = result

    def generate(self, output_path: Path) -> None:
        data = self._build_data()
        template = jinja2.Template(TEMPLATE_PATH.read_text())
        html = template.render(
            run_id=self.result.run_id,
            data_json=json.dumps(data, default=str),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html)
        self._print_summary(data)

    def _build_data(self) -> dict:
        signal_posts = []
        total_correct = 0
        total_evaluated = 0

        for pr in self.result.results:
            if pr.recommendation.action != "signal":
                continue

            predictions = []
            for o in pr.outcomes:
                predictions.append({
                    "asset": o.prediction.asset,
                    "direction": o.prediction.direction,
                    "timeframe": o.prediction.timeframe,
                    "confidence": o.prediction.confidence,
                    "actual_change_pct": o.actual_change_pct,
                    "direction_correct": o.direction_correct,
                    "price_at_post": o.actual_price_at_post,
                    "price_after": o.actual_price_after,
                })
                total_evaluated += 1
                if o.direction_correct:
                    total_correct += 1

            chart_data = {}
            for asset, candles in pr.chart_candles.items():
                chart_data[asset] = {
                    "candles": [
                        [c.timestamp_ms, c.open, c.high, c.low, c.close]
                        for c in candles
                    ],
                    "post_timestamp_ms": pr.post.created_at_ms,
                }

            signal_posts.append({
                "post_id": pr.post.id,
                "post_date": pr.post.created_at.strftime("%Y-%m-%d %H:%M UTC"),
                "post_text": pr.post.text,
                "engagement": pr.post.engagement,
                "importance": pr.recommendation.importance_score,
                "reasoning": pr.recommendation.reasoning,
                "market_analysis": pr.recommendation.market_analysis,
                "predictions": predictions,
                "chart_data": chart_data,
            })

        direction_accuracy = (total_correct / total_evaluated * 100) if total_evaluated else 0

        return {
            "summary": {
                "posts_total": self.result.posts_total,
                "posts_with_signal": self.result.posts_with_signal,
                "signal_rate_pct": (
                    self.result.posts_with_signal / self.result.posts_total * 100
                    if self.result.posts_total else 0
                ),
                "direction_accuracy_pct": direction_accuracy,
                "total_cost": self.result.total_agent_cost,
                "total_correct": total_correct,
                "total_evaluated": total_evaluated,
            },
            "signal_posts": signal_posts,
        }

    def _print_summary(self, data: dict) -> None:
        console = Console()
        s = data["summary"]

        table = Table(title="Backtest Summary", show_header=False)
        table.add_column("Metric", style="bold")
        table.add_column("Value")

        table.add_row("Posts total", str(s["posts_total"]))
        table.add_row("Signals", f"{s['posts_with_signal']} ({s['signal_rate_pct']:.1f}%)")
        table.add_row("Direction accuracy", f"{s['direction_accuracy_pct']:.0f}% ({s['total_correct']}/{s['total_evaluated']})")
        table.add_row("Agent cost", f"${s['total_cost']:.2f}")

        console.print(table)
