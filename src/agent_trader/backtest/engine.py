import asyncio
import json
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx
import weave
from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from loguru import logger

from agent_trader.agent.prompts import build_system_prompt, build_user_prompt
from agent_trader.backtest.evaluator import Evaluator
from agent_trader.config import BacktestConfig
from agent_trader.data.market import fetch_candles_for_chart
from agent_trader.data.news import NewsArchive, get_news_context, load_news
from agent_trader.data.posts import load_posts
from agent_trader.models.outcome import BacktestResult, PostResult
from agent_trader.models.post import TruthPost
from agent_trader.models.recommendation import Recommendation
from agent_trader.reporting.html_report import HtmlReport

RECOMMENDATION_SCHEMA = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["signal", "skip"]},
            "reasoning": {"type": "string"},
            "importance_score": {"type": "integer"},
            "market_analysis": {"type": "string"},
            "predictions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "asset": {"type": "string"},
                        "direction": {"type": "string", "enum": ["up", "down"]},
                        "timeframe": {"type": "string", "enum": ["5m", "15m", "30m", "1h", "4h"]},
                        "confidence": {"type": "string", "enum": ["high", "very_high"]},
                    },
                    "required": ["asset", "direction", "timeframe", "confidence"],
                },
            },
        },
        "required": ["action", "reasoning", "importance_score", "predictions"],
    },
}


@dataclass
class Worker:
    port: int
    proxy_proc: subprocess.Popen | None = None


@dataclass
class PreparedPost:
    post: TruthPost
    prev_posts: list[TruthPost]
    news_context: str


class BacktestEngine:
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.evaluator = Evaluator()
        self.system_prompt = build_system_prompt()
        self.ca_cert = str(Path(config.proxy_ca_cert).expanduser())

    async def run(self, start_dt=None, end_dt=None):
        weave.init(self.config.wandb_project)

        posts = load_posts(self.config.posts_path, start_dt, end_dt)
        if not posts:
            logger.warning("No posts to process")
            return

        news = load_news(self.config.news_path)
        prepared = self._prepare_posts(posts, news)

        workers = self._create_workers()
        try:
            results = await self._process_all(prepared, workers)
        finally:
            self._stop_proxies(workers)

        backtest_result = BacktestResult(
            run_id=uuid.uuid4().hex[:12],
            posts_total=len(posts),
            posts_with_signal=sum(1 for r in results if r.recommendation.action == "signal"),
            posts_skipped=sum(1 for r in results if r.recommendation.action == "skip"),
            results=results,
            start_date=posts[0].created_at,
            end_date=posts[-1].created_at,
            total_agent_cost=sum(r.agent_cost_usd for r in results),
        )

        output_path = Path(self.config.results_dir) / f"{backtest_result.run_id}.html"
        HtmlReport(backtest_result).generate(output_path)
        logger.info(f"Report saved to {output_path}")

    def _prepare_posts(self, posts: list[TruthPost], news: NewsArchive) -> list[PreparedPost]:
        prepared = []
        for i, post in enumerate(posts):
            prev = posts[max(0, i - 5):i]
            ctx = get_news_context(news, post.created_at)
            prepared.append(PreparedPost(post=post, prev_posts=prev, news_context=ctx))
        return prepared

    def _create_workers(self) -> list[Worker]:
        workers = []
        for i in range(self.config.concurrency):
            port = self.config.proxy_base_port + i
            proc = self._start_proxy(port)
            workers.append(Worker(port=port, proxy_proc=proc))
        return workers

    def _start_proxy(self, port: int) -> subprocess.Popen | None:
        addon_path = Path(self.config.proxy_addon_path)
        if not addon_path.exists():
            logger.warning(f"Proxy addon not found at {addon_path}, skipping proxy start")
            return None

        proc = subprocess.Popen(
            [
                "mitmdump",
                "--listen-port", str(port),
                "--set", f"data_dir={self.config.proxy_data_dir}",
                "-s", str(addon_path),
                "--quiet",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"Started proxy on port {port} (pid={proc.pid})")
        return proc

    def _stop_proxies(self, workers: list[Worker]):
        for w in workers:
            if w.proxy_proc and w.proxy_proc.poll() is None:
                w.proxy_proc.terminate()
                w.proxy_proc.wait(timeout=5)

    def _build_options(self, worker: Worker) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            model=self.config.model,
            system_prompt=self.system_prompt,
            tools=["Bash"],
            allowed_tools=["Bash"],
            disallowed_tools=["Read", "Write", "Edit", "Glob", "Grep", "WebSearch", "WebFetch"],
            permission_mode="bypassPermissions",
            max_budget_usd=self.config.max_budget_per_post_usd,
            output_format=RECOMMENDATION_SCHEMA,
            sandbox={
                "enabled": True,
                "autoAllowBashIfSandboxed": True,
                "network": {
                    "httpProxyPort": worker.port,
                },
            },
            env={
                "SSL_CERT_FILE": self.ca_cert,
                "REQUESTS_CA_BUNDLE": self.ca_cert,
            },
            cwd=str(Path.cwd()),
        )

    async def _process_all(self, prepared: list[PreparedPost], workers: list[Worker]) -> list[PostResult]:
        pool: asyncio.Queue[Worker] = asyncio.Queue()
        for w in workers:
            pool.put_nowait(w)

        sem = asyncio.Semaphore(self.config.concurrency)

        async def process_one(pp: PreparedPost) -> PostResult:
            async with sem:
                worker = await pool.get()
                try:
                    return await self._process_post(pp, worker)
                finally:
                    pool.put_nowait(worker)

        tasks = [process_one(pp) for pp in prepared]
        return list(await asyncio.gather(*tasks))

    async def _process_post(self, pp: PreparedPost, worker: Worker) -> PostResult:
        post = pp.post

        if worker.proxy_proc:
            try:
                async with httpx.AsyncClient() as ctl:
                    await ctl.post(
                        f"http://localhost:{worker.port}/__control/set_time",
                        json={"timestamp_ms": post.created_at_ms},
                    )
            except Exception as e:
                logger.warning(f"Failed to set proxy time for post {post.id}: {e}")

        user_prompt = build_user_prompt(post, pp.prev_posts, pp.news_context)
        options = self._build_options(worker)

        cost = 0.0
        turns = 0
        structured_output = None
        try:
            async for msg in query(prompt=user_prompt, options=options):
                if isinstance(msg, ResultMessage):
                    cost = msg.total_cost_usd or 0.0
                    turns = msg.num_turns or 0
                    structured_output = msg.structured_output
        except Exception as e:
            logger.error(f"Agent error on post {post.id}: {e}")

        rec = self._parse_recommendation(structured_output)

        logger.info(
            f"Post {post.id} [{post.created_at:%Y-%m-%d %H:%M}]: "
            f"action={rec.action} importance={rec.importance_score} "
            f"cost=${cost:.2f} turns={turns}"
        )
        if rec.action == "signal":
            for p in rec.predictions:
                logger.info(f"  → {p.asset} {p.direction} {p.timeframe} ({p.confidence})")
        logger.info(f"  reasoning: {rec.reasoning[:200]}")

        outcomes = []
        chart_candles: dict = {}

        if rec.action == "signal":
            async with httpx.AsyncClient(timeout=30) as direct_client:
                outcomes = await self.evaluator.evaluate(rec, post, direct_client)
                for pred in rec.predictions:
                    try:
                        candles = await fetch_candles_for_chart(
                            pred.asset, post.created_at_ms, pred.timeframe, direct_client
                        )
                        chart_candles[pred.asset] = candles
                    except Exception as e:
                        logger.warning(f"Failed to fetch chart for {pred.asset}: {e}")

        return PostResult(
            post=post,
            recommendation=rec,
            outcomes=outcomes,
            agent_cost_usd=cost,
            agent_turns=turns,
            chart_candles=chart_candles,
        )

    def _parse_recommendation(self, structured_output) -> Recommendation:
        if structured_output is None:
            return Recommendation(
                action="skip",
                reasoning="Agent did not produce structured output",
                importance_score=1,
                predictions=[],
            )

        try:
            data = structured_output if isinstance(structured_output, dict) else json.loads(structured_output)
            return Recommendation.model_validate(data)
        except Exception as e:
            logger.warning(f"Failed to parse recommendation: {e} | raw: {str(structured_output)[:300]}")
            return Recommendation(
                action="skip",
                reasoning=f"Failed to parse: {e}",
                importance_score=1,
                predictions=[],
            )
