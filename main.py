"""Gen 1 Cloud Function entry point, matching pyspace-minimal's own
root main.py exactly.

Deploy with:
    gcloud functions deploy pylogma-serverless \
        --no-gen2 \
        --runtime python312 \
        --entry-point main \
        --trigger-http \
        --set-env-vars ROUTER_MODULE=router,REDIS_URI=...,API_KEY=...

See README.md for the full env var list and local run instructions.
"""

from os import path

from cloud_function_app import CloudFunctionApp

app = CloudFunctionApp(root=path.dirname(path.abspath(__file__)))
main = app.build()
