# W&B Weave Python API Reference

How to fetch traces from Weave programmatically for the `frequensy/agent-trader` project.

**Weave version tested**: 0.52.35
**Dashboard**: https://wandb.ai/frequensy/agent-trader/weave

## Setup

```python
import weave

# WANDB_API_KEY must be in environment (or .env)
client = weave.init("frequensy/agent-trader")
```

`weave.init()` returns a `WeaveClient` instance. It also registers the project for tracing (ops decorated with `@weave.op` will auto-log to this project).

## Listing Calls

### Recent root-level traces

```python
calls = client.get_calls(
    filter={"trace_roots_only": True},
    sort_by=[{"field": "started_at", "direction": "desc"}],
    limit=10,
)
for c in calls:
    print(c.id, c.op_name, c.started_at, c.display_name)
```

### Get all children of a trace

```python
children = client.get_calls(
    filter={"trace_ids": ["019d2081-a8f1-7732-9fbe-4ec7f51481e6"]},
    sort_by=[{"field": "started_at", "direction": "asc"}],
)
```

### Filter by op name

```python
calls = client.get_calls(
    filter={"op_names": ["weave:///frequensy/agent-trader/op/my_op:hash"]},
    limit=50,
)
```

### Filter by parent (direct children of a call)

```python
children = client.get_calls(
    filter={"parent_ids": ["019d2081-a8f1-7768-b304-47f9e7900e09"]},
)
```

## Getting a Single Call

```python
call = client.get_call(
    "019d2081-a8f1-7768-b304-47f9e7900e09",
    include_costs=True,
    include_feedback=True,
)
```

## Call Data Structure

Each call is a dataclass with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Unique call ID (UUID) |
| `trace_id` | `str` | Groups all calls in one execution tree |
| `project_id` | `str` | `entity/project` format |
| `parent_id` | `str \| None` | Parent call ID (`None` for root) |
| `op_name` | `str` | Full weave ref to the op, e.g. `weave:///entity/project/op/name:hash` |
| `display_name` | `str \| None` | Human-readable name |
| `inputs` | `dict` | Input arguments to the op |
| `output` | `Any` | Return value (often a `WeaveDict`) |
| `exception` | `str \| None` | Error message if the call failed |
| `summary` | `dict \| None` | Aggregated stats (usage, costs, status_counts) |
| `attributes` | `dict \| None` | Metadata (weave client version, OS, custom attrs) |
| `started_at` | `datetime` | When the call started (UTC) |
| `ended_at` | `datetime \| None` | When the call ended (`None` if still running) |
| `thread_id` | `str \| None` | Thread identifier |
| `turn_id` | `str \| None` | Turn identifier |
| `_children` | `list[Call]` | Child calls (may not be populated by default) |

## Summary Structure

The `summary` dict on root calls typically contains:

```python
{
    "usage": {
        "claude-opus-4-6": {
            "input_tokens": 14,
            "output_tokens": 5765,
            "requests": 1,
            "cache_read_input_tokens": 142424,
            "cache_creation_input_tokens": 11283,
        }
    },
    "status_counts": {"success": 25, "error": 0},
    "weave": {
        "status": "success",
        "latency_ms": 109707,
        "costs": {
            "claude-opus-4-6": {
                "prompt_tokens": 14,
                "completion_tokens": 5765,
                "prompt_tokens_total_cost": 0.00007,
                "completion_tokens_total_cost": 0.1441,
            }
        }
    }
}
```

## CallsFilter Fields

Pass as a dict or `CallsFilter` instance to `get_calls(filter=...)`:

| Field | Type | Description |
|-------|------|-------------|
| `op_names` | `list[str]` | Filter by op ref URIs |
| `trace_ids` | `list[str]` | Filter by trace ID |
| `call_ids` | `list[str]` | Filter by specific call IDs |
| `parent_ids` | `list[str]` | Filter by parent call ID |
| `trace_roots_only` | `bool` | Only return root calls (no parent) |
| `input_refs` | `list[str]` | Filter by input refs |
| `output_refs` | `list[str]` | Filter by output refs |
| `wb_user_ids` | `list[str]` | Filter by W&B user IDs |
| `wb_run_ids` | `list[str]` | Filter by W&B run IDs |

## Sorting

```python
calls = client.get_calls(
    sort_by=[{"field": "started_at", "direction": "desc"}],
)
```

`direction` is `"asc"` or `"desc"`. Common sortable fields: `started_at`, `ended_at`.

## Advanced Queries (Mongo-style)

The `query` parameter accepts Mongo-style expressions:

```python
# Filter where display_name contains "backtest"
calls = client.get_calls(
    query={
        "$expr": {
            "$contains": {
                "input": {"$getField": "display_name"},
                "substr": {"$literal": "backtest"},
                "case_insensitive": True,
            }
        }
    }
)

# Filter by exact op_name
calls = client.get_calls(
    query={
        "$expr": {
            "$eq": [
                {"$getField": "op_name"},
                {"$literal": "predict"},
            ]
        }
    }
)
```

## Column Projection (Performance)

Specify `columns` to only fetch needed fields:

```python
calls = client.get_calls(
    columns=["id", "trace_id", "op_name", "started_at", "display_name", "summary"],
    limit=100,
)
```

Fields `id`, `trace_id`, `op_name`, and `started_at` are always included.

## Converting to Pandas

```python
df = client.get_calls(
    filter={"trace_roots_only": True},
    limit=100,
).to_pandas()
```

## Child Span Types (Claude Agent SDK)

When tracing Claude Agent SDK runs, child spans have these op name patterns:

| Op name suffix | Description |
|---------------|-------------|
| `claude_agent_sdk.thinking` | Model thinking/reasoning block |
| `claude_agent_sdk.text` | Model text output |
| `claude_agent_sdk.tool_use.Bash` | Bash tool call |
| `claude_agent_sdk.tool_use.mcp-trading-submit_recommendation` | MCP tool call |

For tool calls:
- `inputs["message"]` contains the tool invocation details
- `output["tool_use_id"]` is the tool use ID
- `output["content"]` is the tool result
- `output["is_error"]` indicates if the tool call failed

For thinking spans:
- `output["thinking"]` contains the thinking text

For text spans:
- `output["text"]` contains the text content
- `output["model"]` contains the model name

## Working Script

See `scripts/fetch_weave_trace.py` for a complete working example that:
- Lists recent root traces
- Fetches and prints a full trace tree with children
- Shows usage stats, costs, tool calls, thinking, and text spans

```bash
# Show the latest trace:
python scripts/fetch_weave_trace.py

# Show a specific trace:
python scripts/fetch_weave_trace.py 019d2081-a8f1-7768-b304-47f9e7900e09

# List last 5 traces:
python scripts/fetch_weave_trace.py --last 5

# Verbose mode (show inputs/outputs):
python scripts/fetch_weave_trace.py -v
```
