"""Route registration for the pylogma-serverless router package.

pyspace-minimal's ROUTES = {url_rule: view_func} mechanism only
registers GET/HEAD/OPTIONS routes -- it has no way to ask for POST.
/run needs POST (it triggers a blocking, side-effecting operation, not
a GET-safe read), so it's registered directly on Flask's current_app
instead, with methods=["POST"].

This is safe, not a hack: pyspace-minimal imports this module (via
ROUTER_MODULE) only after it has already built its Flask app and pushed
an app context for it, so current_app here IS that same app object.
(pyspace-minimal used to offer a register(app) hook for exactly this
before it was replaced by the ROUTES dict -- this is the modern
equivalent, just triggered by import instead of an explicit call.)

This package never owns a Cloud Function entry point itself -- see
README.md for how a separate deploy-shell repo (e.g. xd-dash/huram-abi)
installs this package and supplies pyspace-minimal's own main.py.
"""

from flask import current_app

from pylogma_serverless.views import run_view

current_app.add_url_rule("/run", endpoint="/run", view_func=run_view, methods=["POST"])

# Nothing registered through the declarative mechanism -- /run needs
# POST, which register_routes()'s add_url_rule call doesn't support.
# Left present (rather than omitted) so it's clear this module was
# checked against the ROUTER_MODULE contract, not just skipped.
ROUTES: dict = {}
