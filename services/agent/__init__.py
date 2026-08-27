"""agentlake CLI agent -- a bounded tool-use loop over the inference gateway
and services/mcp_server (spawned as a real MCP client over stdio).

Run with ``python -m services.agent "question" [--session ID] [--quality]``.
See docs/adr/ADR-003.
"""
