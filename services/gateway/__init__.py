"""agentlake inference gateway -- the only door for LLM calls in this codebase.

    from services.gateway import create_app
"""

from services.gateway.app import create_app

__all__ = ["create_app"]
