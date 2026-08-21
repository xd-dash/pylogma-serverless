"""pylogma_serverless: single-request, instance-local, bounded-lifetime
Redis Pub/Sub runtime, for Google Cloud Functions Gen 1.

A threaded/synchronous port of the Go `router` package in
xd-dash/logma-serverless, dispatched via pyspace-minimal's
CloudFunctionApp/ROUTER_MODULE mechanism (see router.py). See
runtime.py's module docstring for the full design description and the
Go-to-Python concept mapping.
"""

from .runtime import Runtime, RuntimeHolder, RuntimeState

__all__ = ["Runtime", "RuntimeHolder", "RuntimeState"]
