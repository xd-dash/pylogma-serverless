"""Entrypoint for local/uvicorn and Cloud Run/Cloud Functions Gen 2 deploys.

Run locally:
    uvicorn main:app --host 0.0.0.0 --port 8080

Cloud Run deploy note: set --concurrency=1 (the Python equivalent of the Go
version's maxInstanceRequestConcurrency=1) so exactly one HTTP request --
and therefore exactly one live Runtime -- is ever in flight per container
instance. Cloud Functions Gen 2 runs on Cloud Run under the hood and takes
the same setting.
"""

from pylogma_serverless.app import app

__all__ = ["app"]
