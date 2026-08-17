"""ROUTER_MODULE target for pyspace-minimal's CloudFunctionApp (see
README.md's "Gen 1 deploy" section for how ROUTER_MODULE is wired up).

pyspace-minimal's declarative `ROUTES = {rule: view_func}` merge
(`CloudFunctionApp.register_routes`) calls `add_url_rule(rule,
endpoint=rule, view_func=view_func)` without a `methods=` argument, so
routes registered that way default to GET/HEAD/OPTIONS only -- there's no
way to ask for POST through that mechanism as it stands upstream.

The Go version's route is `POST /run` (it runs a blocking, side-effecting
operation -- claims a runtime and drives it to completion -- which is not
GET-shaped), so it's registered directly on the shared Flask app instead
of through ROUTES. This still works within pyspace-minimal's own model:
`CloudFunctionApp.build_app()` sets `self.app = current_app`, resolving
to the *same* Flask app functions-framework already built and pushed an
app context for; `load_router_routes()` imports this module from inside
that same pushed context (during `CloudFunctionApp.build()`), so
`flask.current_app` here resolves to that identical app object.
"""

from flask import current_app

from pylogma_serverless.views import run_view

current_app.add_url_rule("/run", endpoint="/run", view_func=run_view, methods=["POST"])

# Nothing registered through the declarative mechanism on this branch --
# /run needs POST, which register_routes()'s add_url_rule call doesn't
# support. Left present (rather than omitted) so it's clear this module
# was checked against the ROUTER_MODULE contract, not just skipped.
ROUTES: dict = {}
