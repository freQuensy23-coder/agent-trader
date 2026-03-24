import asyncio
from datetime import datetime, timezone

import fire

from agent_trader.backtest.engine import BacktestEngine
from agent_trader.config import BacktestConfig


def run(start: str | None = None, end: str | None = None, **overrides):
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc) if start else None
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc) if end else None

    config = BacktestConfig(**overrides)
    asyncio.run(BacktestEngine(config).run(start_dt, end_dt))


if __name__ == "__main__":
    fire.Fire(run)
