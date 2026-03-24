from typing import Any

from claude_agent_sdk import create_sdk_mcp_server, tool
from loguru import logger

from agent_trader.models.recommendation import Recommendation

SUBMIT_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["signal", "skip"],
            "description": "Your decision: 'signal' if the post could move markets, 'skip' otherwise.",
        },
        "reasoning": {
            "type": "string",
            "minLength": 20,
            "description": "Why you made this decision. Be specific.",
        },
        "importance_score": {
            "type": "integer",
            "minimum": 1,
            "maximum": 10,
            "description": "How important is this post for markets? 1=irrelevant, 10=market-moving.",
        },
        "market_analysis": {
            "type": "string",
            "description": "Brief summary of market conditions you checked. Fill BEFORE predictions.",
        },
        "predictions": {
            "type": "array",
            "maxItems": 3,
            "description": "Your predictions. Empty array if action='skip'.",
            "items": {
                "type": "object",
                "properties": {
                    "asset": {
                        "type": "string",
                        "description": "HyperLiquid perp ticker (e.g. BTC, ETH, SOL).",
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["up", "down"],
                    },
                    "timeframe": {
                        "type": "string",
                        "enum": ["5m", "15m", "30m", "1h", "4h"],
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "very_high"],
                    },
                },
                "required": ["asset", "direction", "timeframe", "confidence"],
            },
        },
    },
    "required": ["action", "reasoning", "importance_score", "predictions"],
}


def create_recommendation_tool(captured: dict[str, Recommendation | None]):
    """Create MCP tool + server. `captured["rec"]` is set when the agent calls the tool."""

    @tool(
        "submit_recommendation",
        "Submit your final recommendation. Call ONCE after analysis. Fill market_analysis FIRST, then predictions.",
        SUBMIT_TOOL_SCHEMA,
    )
    async def handle_submit(args: dict[str, Any]) -> dict[str, Any]:
        logger.info(f"submit_recommendation called with: {args}")
        try:
            rec = Recommendation.model_validate(args)
        except Exception as e:
            logger.warning(f"submit_recommendation validation failed: {e}")
            return {
                "content": [{"type": "text", "text": f"Validation error: {e}. Fix and retry."}],
                "is_error": True,
            }

        captured["rec"] = rec
        logger.info(f"submit_recommendation captured: action={rec.action} importance={rec.importance_score}")
        return {
            "content": [{"type": "text", "text": f"Recommendation recorded: {rec.action}"}],
        }

    server = create_sdk_mcp_server(
        "trading",
        version="1.0.0",
        tools=[handle_submit],
    )
    return server
