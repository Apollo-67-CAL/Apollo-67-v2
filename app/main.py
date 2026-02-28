"""Compatibility shim for existing deployment entrypoints.

Render/CLI can continue using app.main:app while the main FastAPI app lives in api.main.
"""

from api.main import app

__all__ = ["app"]
