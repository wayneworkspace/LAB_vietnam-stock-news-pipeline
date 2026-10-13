"""Entry point for uvicorn (server:app). The API code lives in serving/api.py."""
import logging

from serving.api import app  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
