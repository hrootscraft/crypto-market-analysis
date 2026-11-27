"""Run with `python -m mcp_server` (stdio transport, for Claude Desktop / Claude Code)."""

from .server import mcp

if __name__ == "__main__":
    mcp.run()
