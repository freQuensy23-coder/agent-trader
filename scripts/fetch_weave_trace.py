#!/usr/bin/env python3
"""Fetch and print a full Weave trace from W&B.

Usage:
    # Fetch the last root-level trace:
    python scripts/fetch_weave_trace.py

    # Fetch a specific call by ID:
    python scripts/fetch_weave_trace.py 019d2081-a8f1-7768-b304-47f9e7900e09

    # Fetch last N root traces:
    python scripts/fetch_weave_trace.py --last 5

Requires WANDB_API_KEY in .env or environment.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import weave  # noqa: E402  (must come after env loading)


PROJECT = "frequensy/agent-trader"


def _short(text: str | None, max_len: int = 120) -> str:
    if text is None:
        return "<none>"
    s = str(text).replace("\n", " ")
    return s[:max_len] + "..." if len(s) > max_len else s


def _duration(call) -> str:
    if call.started_at and call.ended_at:
        secs = (call.ended_at - call.started_at).total_seconds()
        if secs < 1:
            return f"{secs*1000:.0f}ms"
        return f"{secs:.1f}s"
    return "running" if call.started_at and not call.ended_at else "?"


def _op_short_name(op_name: str) -> str:
    """Extract short op name from full weave ref."""
    # weave:///entity/project/op/name:hash -> name
    if "/op/" in op_name:
        name = op_name.split("/op/")[-1].split(":")[0]
        # claude_agent_sdk.tool_use.Bash -> tool_use.Bash
        if "claude_agent_sdk." in name:
            name = name.replace("claude_agent_sdk.", "")
        return name
    return op_name


def print_call_tree(
    client: weave.WeaveClient,
    root_call_id: str,
    *,
    verbose: bool = False,
) -> None:
    """Print a full trace tree for a root call."""
    # Get the root call with costs
    root = client.get_call(root_call_id, include_costs=True)
    trace_id = root.trace_id

    print(f"{'='*80}")
    print(f"TRACE: {root.display_name or root.op_name}")
    print(f"  Call ID:    {root.id}")
    print(f"  Trace ID:   {trace_id}")
    print(f"  Op:         {_op_short_name(root.op_name)}")
    print(f"  Started:    {root.started_at}")
    print(f"  Duration:   {_duration(root)}")
    print(f"  Status:     {'ERROR' if root.exception else 'success'}")

    # Summary/usage
    if root.summary:
        usage = root.summary.get("usage", {})
        for model, stats in usage.items():
            in_tok = stats.get("input_tokens", 0)
            out_tok = stats.get("output_tokens", 0)
            cache_read = stats.get("cache_read_input_tokens", 0)
            cache_create = stats.get("cache_creation_input_tokens", 0)
            reqs = stats.get("requests", 0)
            print(f"  Usage ({model}): {reqs} req, {in_tok} in, {out_tok} out, "
                  f"{cache_read} cache_read, {cache_create} cache_create")

        costs = root.summary.get("weave", {}).get("costs", {})
        for model, cost_info in costs.items():
            total = (cost_info.get("prompt_tokens_total_cost", 0) +
                     cost_info.get("completion_tokens_total_cost", 0))
            print(f"  Cost ({model}): ${total:.4f}")

    # Inputs
    if root.inputs and verbose:
        print(f"\n  INPUTS:")
        for k, v in root.inputs.items():
            print(f"    {k}: {_short(str(v), 200)}")

    # Output
    if root.output and verbose:
        print(f"\n  OUTPUT: {_short(str(root.output), 300)}")

    # Get all child calls in the trace
    children = client.get_calls(
        filter={"trace_ids": [trace_id]},
        sort_by=[{"field": "started_at", "direction": "asc"}],
    )

    # Build parent -> children map
    all_calls = list(children)
    children_map: dict[str | None, list] = {}
    for c in all_calls:
        children_map.setdefault(c.parent_id, []).append(c)

    # Print tree
    child_count = len(all_calls) - 1  # exclude root
    print(f"\n  Child spans: {child_count}")
    print(f"{'='*80}")

    def print_subtree(parent_id: str | None, depth: int = 0):
        for c in children_map.get(parent_id, []):
            if c.id == root_call_id and depth == 0:
                continue  # skip root in tree
            indent = "  " * (depth + 1)
            op = _op_short_name(c.op_name)
            status = "ERR" if c.exception else "ok"
            dur = _duration(c)

            # Determine span type for display
            display = c.display_name or ""
            if "tool_use" in op:
                tool_name = op.replace("tool_use.", "")
                print(f"{indent}[TOOL] {tool_name} ({dur}, {status})")
                if c.output:
                    out = str(c.output)
                    is_error = "'is_error': True" in out or "'is_error': true" in out
                    if is_error:
                        print(f"{indent}  -> ERROR: {_short(str(c.output), 200)}")
                    elif verbose:
                        print(f"{indent}  -> {_short(str(c.output), 200)}")
            elif "thinking" in op:
                thinking_text = ""
                if c.output and hasattr(c.output, "get"):
                    thinking_text = c.output.get("thinking", "")
                elif c.output:
                    thinking_text = str(c.output)
                print(f"{indent}[THINK] {_short(thinking_text, 150)}")
            elif "text" in op:
                text_content = ""
                if c.output and hasattr(c.output, "get"):
                    text_content = c.output.get("text", "")
                elif c.output:
                    text_content = str(c.output)
                print(f"{indent}[TEXT] {_short(text_content, 150)}")
            else:
                print(f"{indent}[{op}] {_short(display, 120)} ({dur}, {status})")

            # Recurse into children
            print_subtree(c.id, depth + 1)

    print_subtree(root_call_id)
    print()


def list_recent_roots(client: weave.WeaveClient, limit: int = 5) -> list:
    """List recent root-level traces."""
    calls = client.get_calls(
        filter={"trace_roots_only": True},
        sort_by=[{"field": "started_at", "direction": "desc"}],
        limit=limit,
        columns=["id", "trace_id", "op_name", "started_at", "ended_at",
                 "display_name", "summary", "exception"],
    )
    results = list(calls)
    print(f"\nRecent {len(results)} root traces:")
    print(f"{'-'*80}")
    for i, c in enumerate(results):
        status = "ERR" if c.exception else "ok "
        dur = _duration(c)
        display = c.display_name or _op_short_name(c.op_name)
        print(f"  [{i}] {status} {dur:>8}  {c.started_at:%Y-%m-%d %H:%M:%S}  "
              f"{c.id}  {_short(display, 60)}")
    print()
    return results


def main():
    parser = argparse.ArgumentParser(description="Fetch Weave traces")
    parser.add_argument("call_id", nargs="?", help="Specific call ID to fetch")
    parser.add_argument("--last", type=int, default=1,
                        help="Number of recent root traces to show (default: 1)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show inputs/outputs for all spans")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON instead of tree")
    args = parser.parse_args()

    import warnings
    warnings.filterwarnings("ignore")

    client = weave.init(PROJECT)

    if args.call_id:
        # Fetch specific call
        print_call_tree(client, args.call_id, verbose=args.verbose)
    else:
        # List recent and print the latest
        roots = list_recent_roots(client, limit=args.last)
        if roots:
            print_call_tree(client, roots[0].id, verbose=args.verbose)


if __name__ == "__main__":
    main()
