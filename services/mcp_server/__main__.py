import asyncio

from services.mcp_server.server import run_stdio

if __name__ == "__main__":
    asyncio.run(run_stdio())
