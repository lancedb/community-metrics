# Container image for the scheduled ingestion jobs on AWS Lambda.
# Entry point: community_metrics.lambda_handler.handler (see that module for the event shape).
#
# Build:  docker build -t community-metrics-jobs .
# Local:  docker run --rm -p 9000:8080 --env-file <env file without quotes> community-metrics-jobs
#         curl -s localhost:9000/2015-03-31/functions/function/invocations -d '{"job": "daily"}'

FROM ghcr.io/astral-sh/uv:0.12.7 AS uv

FROM public.ecr.aws/lambda/python:3.13

# Install the exact locked runtime dependencies (no dev deps), with hash checking.
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/build/
RUN cd /tmp/build \
    && uv export --frozen --no-dev --no-emit-project --format requirements-txt -o requirements.txt \
    && pip install --no-cache-dir --require-hashes -r requirements.txt --target "${LAMBDA_TASK_ROOT}" \
    && rm -rf /tmp/build /usr/local/bin/uv

# Only the Python package is needed. seed_data/ is used by bootstrap/backfill jobs,
# which are not allowed to run from Lambda.
COPY src/community_metrics "${LAMBDA_TASK_ROOT}/community_metrics"

CMD ["community_metrics.lambda_handler.handler"]
