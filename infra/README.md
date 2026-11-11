# infra: scheduled jobs on AWS Lambda

AWS CDK (Python) stack `CommunityMetricsJobs`. It runs the ingestion jobs from the
repo-root `Dockerfile` as one Lambda, on a schedule. The dashboard (Vercel) is not part of it.

| Resource | Details |
| --- | --- |
| Lambda | Container image built from the repo root, arm64, 1024 MB, 15 min timeout, one run at a time, failed runs retried twice |
| Secrets Manager secret | `community-metrics/prod`: JSON of env vars, filled in by hand. Kept if the stack is deleted |
| EventBridge Scheduler | `daily` 11:00 UTC, `weekly-guidance` Mon 11:30 UTC, `monthly-duckdb` 1st 12:00 UTC. Created **disabled** unless `schedulesEnabled=true` |
| CloudWatch Logs | 90-day retention |
| Alarm + SNS | Emails `alertEmails` when any run fails |

The image is built locally by `cdk deploy` and pushed to the CDK bootstrap ECR repo, so
deploying needs Docker (Docker Desktop, Colima or OrbStack).

## Requirements

- Docker running, Node.js (for `npx aws-cdk`), `uv`, and AWS credentials for the target account.
- The account/region bootstrapped once: `npx aws-cdk bootstrap aws://054483968661/us-east-2`.

## Deploy

```bash
cd infra
uv sync
npx aws-cdk diff  -c alertEmails=you@lancedb.com
npx aws-cdk deploy -c alertEmails=you@lancedb.com
```

Context options (`-c name=value`):

| Name | Default | Meaning |
| --- | --- | --- |
| `region` | `us-east-2` | AWS region (same as devrel-proxy) |
| `alertEmails` | none | Comma-separated emails for failure alerts. Each address must confirm the SNS email |
| `secretName` | `community-metrics/prod` | Secrets Manager secret the Lambda reads |
| `schedulesEnabled` | `false` | Set `true` once the secret is filled in and a manual run works |

## First-time setup

1. Deploy with schedules disabled (the default).
2. Fill in the secret with the same keys as `.env` (only the job keys, not the dashboard's
   `GOOGLE_*` / `NEXTAUTH_*`). In the console: Secrets Manager → `community-metrics/prod` →
   Retrieve secret value → Edit → Plaintext:

   ```json
   {
     "LANCEDB_API_KEY": "...",
     "LANCEDB_HOST_OVERRIDE": "...",
     "LANCEDB_REGION": "...",
     "GITHUB_TOKEN": "...",
     "OPENAI_API_KEY": "..."
   }
   ```

   Never put these values in code, the CDK context, or the shell history.
3. Run once by hand: Lambda console → the `CommunityMetricsJobs-JobsFunction…` function → Test
   with event `{"job": "daily"}`. Check the logs; the run returns `"ok": true`.
4. Turn the schedules on: `npx aws-cdk deploy -c alertEmails=... -c schedulesEnabled=true`.

Always pass the same `-c` values on every deploy. A deploy without `schedulesEnabled=true`
turns the schedules back off.

## Tests

```bash
cd infra
uv run pytest
```
