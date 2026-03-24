from pydantic_settings import BaseSettings, SettingsConfigDict


class BacktestConfig(BaseSettings):
    # Anthropic
    anthropic_api_key: str = ""

    # Agent
    model: str = "claude-opus-4-6"
    max_budget_per_post_usd: float | None = None

    # Proxy
    proxy_base_port: int = 8080
    proxy_addon_path: str = "src/agent_trader/proxy/addon.py"
    proxy_data_dir: str = "data/proxy_snapshots"
    proxy_ca_cert: str = "~/.mitmproxy/mitmproxy-ca-cert.pem"

    # Concurrency
    concurrency: int = 5

    # Data paths
    posts_path: str = "data/posts/trump_posts.parquet"
    news_path: str = "data/news/headlines.json"
    results_dir: str = "data/results"

    # W&B
    wandb_project: str = "agent-trader"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
