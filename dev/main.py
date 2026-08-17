"""Local development harness only -- NOT the Cloud Function deploy entry point.

pylogma-serverless is a router payload, not a deployable Cloud Function on
its own (see pylogma_serverless/router.py's docstring). In production,
some other deploy-shell repo installs this package and supplies
pyspace-minimal's own main.py as the real entry point. This file exists
only so a developer can run, from the repo root:

    ROUTER_MODULE=pylogma_serverless.router \
    REDIS_URI=redis://localhost:6379 \
    API_KEY=dev-key \
    functions-framework --target=main --source=dev/main.py --port 8080

against this repo locally, without needing a separate deploy-shell repo
checked out. It mirrors pyspace-minimal's own root main.py verbatim.
"""

from os import path

from cloud_function_app import CloudFunctionApp

app = CloudFunctionApp(root=path.dirname(path.abspath(__file__)))
main = app.build()
