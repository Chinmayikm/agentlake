"""`python -m services.agent "question" [--session ID] [--quality]`"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid

from services.agent.gateway_client import GatewayUnavailableError, HttpGatewayClient
from services.agent.loop import DEFAULT_MAX_STEPS, DEFAULT_TOOL_TIMEOUT, run_turn
from services.agent.mcp_client import StdioToolExecutor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.agent")
    parser.add_argument("question")
    parser.add_argument(
        "--session", default=None, help="session id to join (generated if omitted)"
    )
    parser.add_argument(
        "--quality", action="store_true", help="use the 'quality' model alias, not 'fast'"
    )
    parser.add_argument("--max-steps", type=int, default=DEFAULT_MAX_STEPS)
    parser.add_argument("--tool-timeout", type=float, default=DEFAULT_TOOL_TIMEOUT)
    return parser


async def _main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Resolved here, not left to run_turn()'s own session(None) auto-generation,
    # because StdioToolExecutor needs the session_id before it spawns the MCP
    # server subprocess -- same id shape as services.sdk._new_id() (ADR-000 #4).
    session_id = args.session or uuid.uuid4().hex

    gateway = HttpGatewayClient()
    try:
        async with StdioToolExecutor(session_id) as tool_executor:
            result = await run_turn(
                args.question,
                gateway=gateway,
                tool_executor=tool_executor,
                session_id=session_id,
                model_alias="quality" if args.quality else "fast",
                max_steps=args.max_steps,
                tool_timeout=args.tool_timeout,
            )
    except GatewayUnavailableError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        await gateway.aclose()

    print(result.answer)
    print(
        f"[{result.steps_used} steps, tools: {result.tools_called}, "
        f"{result.total_tokens} tok, ${result.total_cost_usd:.4f}, "
        f"trace {result.trace_id}]"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
