# infra: scheduled jobs on AWS Lambda

AWS CDK (Python) stack `CommunityMetricsJobs` in account `054483968661`, region `us-east-2`
(same region as devrel-proxy). It runs the ingestion jobs from the repo-root `Dockerfile`
as one Lambda, on a schedule. The dashboard (Vercel) is not part of it.

| Resource | Details |
| --- | --- |
| Lambda | Container image built from the repo root, arm64, 1024 MB, 15 min timeout, one run at a time, failed runs retried twice |
| Secrets Manager secret | `community-metrics/prod`: JSON of env vars, filled in by hand. Kept if the stack is deleted |
| EventBridge Scheduler | `daily` 11:00 UTC, `weekly-guidance` Mon 11:30 UTC, `monthly-duckdb` 1st 12:00 UTC. Each is on only if listed in `enabledSchedules` in `cdk.json` (now `daily` and `monthly-duckdb`; `weekly-guidance` also needs `OPENAI_API_KEY` in the secret) |
| CloudWatch Logs | 90-day retention |
| Alarm + SNS topic `community-metrics-alerts` | Notifies the topic when any run fails |

## How it deploys

GitHub Actions (`.github/workflows/deploy-jobs.yml`) deploys on every merge to `main` that
touches the job code, `Dockerfile`, `uv.lock`/`pyproject.toml` or `infra/`, and can be run by
hand from the Actions tab. It runs the tests, builds the image on an arm64 runner, and runs
`cdk deploy`. Nobody needs Docker locally.

The workflow signs in to AWS with OIDC as `community-metrics-github-deploy`. That role can
only be assumed by this repo's workflows on `refs/heads/main`, and it can only assume the CDK
bootstrap roles in us-east-2 (`cdk-hnb659fds-*-054483968661-us-east-2`).

## One-time setup (already done once per account; listed for rebuilds)

1. CDK bootstrap: `npx aws-cdk bootstrap aws://054483968661/us-east-2`.
2. IAM role `community-metrics-github-deploy`, trusted by the GitHub OIDC provider
   `token.actions.githubusercontent.com` with `sub = repo:lancedb/community-metrics:ref:refs/heads/main`,
   allowed only `sts:AssumeRole` on `arn:aws:iam::054483968661:role/cdk-hnb659fds-*-054483968661-us-east-2` (us-east-2 copies only).
3. After the first deploy:
   - Subscribe alert emails to the `community-metrics-alerts` SNS topic (kept out of code
     because this repo is public). Each address must confirm the email.
   - Fill in the secret `community-metrics/prod` (Secrets Manager console → Retrieve secret
     value → Edit → Plaintext) with the job keys from `.env`:

     ```json
     {
       "LANCEDB_API_KEY": "...",
       "LANCEDB_HOST_OVERRIDE": "...",
       "LANCEDB_REGION": "...",
       "GITHUB_TOKEN": "...",
       "OPENAI_API_KEY": "..."
     }
     ```

     Never put these values in code, `cdk.json` or shell history.
   - Run the Lambda once by hand (console → Test, event `{"job": "daily"}`) and check the logs.
4. Turn schedules on with a PR that adds their names to `"enabledSchedules"` in `cdk.json`.

## Local checks

```bash
cd infra
uv run pytest            # stack tests
npx aws-cdk synth        # build the template (no AWS changes, no Docker needed)
```
