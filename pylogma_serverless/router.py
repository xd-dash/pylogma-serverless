"""Route registration for the pylogma-serverless router package.

This package is not a deployable Cloud Function on its own -- it's a
router payload consumed by some other deploy-shell repo that installs it
(via `pylogma-serverless @ git+https://github.com/xd-dash/pylogma-serverless.git@...`
in that repo's requirements.txt) and sets `ROUTER_MODULE=pylogma_serverless.router`
as a Cloud Function env var. That deploy-shell supplies pyspace-minimal's
own `main.py` (`CloudFunctionApp(...).build()`) as the actual Gen 1 entry
point -- this repo never instantiates `CloudFunctionApp` itself. See
README.md and dev/main.py (a local-dev-only stand-in for that deploy
shell) for the full picture.

pyspace-minimal's `CloudFunctionApp.register_routes()` merges this
module's `ROUTES` dict into the shared Flask app, but that mechanism has
never (in any pyspace-minimal commit) supported specifying HTTP methods
-- routes registered through it are always GET/HEAD/OPTIONS only. Since
`/run` runs a blocking, side-effecting operation and must be POST-only,
it's registered directly on `flask.current_app` here instead. This is
not a workaround: `load_router_routes()` imports this module from
*inside* the same Flask app context `CloudFunctionApp.build_app()`
already pushed (via `self.app = current_app`), so `current_app` here
resolves to that identical app object. It's the current, correct
equivalent of an older `register(app)` hook pyspace-minimal used to
offer for exactly this purpose, before it was replaced by the ROUTES-dict
mechanism (for unrelated route-collision reasons around the default
health-check route) -- just triggered by import side-effect instead of
an explicit call.
"""

from flask import current_app

from pylogma_serverless.views import run_view

current_app.add_url_rule("/run", endpoint="/run", view_func=run_view, methods=["POST"])

# Nothing registered through the declarative mechanism -- /run needs
# POST, which register_routes()'s add_url_rule call doesn't support.
# Left present (rather than omitted) so it's clear this module was
# checked against the ROUTER_MODULE contract, not just skipped.
ROUTES: dict = {}
