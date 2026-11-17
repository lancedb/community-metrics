# infra: scheduled jobs on AWS Lambda, dashboard on the tailnet

AWS CDK (Python) app in account `054483968661`, region `us-east-2` (same region as
devrel-proxy), with two stacks:

- `CommunityMetricsJobs`: the ingestion jobs from the repo-root `Dockerfile` as one Lambda,
  on a schedule.
- `CommunityMetricsDashboard`: the dashboard on one EC2 instance, reachable only over the
  LanceDB tailnet (see [Dashboard host](#dashboard-host) below).

## Jobs

| Resource | Details |
| --- | --- |
| Lambda | Container image built from the repo root, arm64, 1024 MB, 15 min timeout, one run at a time, failed runs retried twice |
| Secrets Manager secret | `community-metrics/prod`: JSON of env vars, filled in by hand. Kept if the stack is deleted |
| EventBridge Scheduler | `daily` 11:00 UTC, `weekly-guidance` Mon 11:30 UTC, `monthly-duckdb` 1st 12:00 UTC. Each is on only if listed in `enabledSchedules` in `cdk.json` (now `daily` and `monthly-duckdb`; `weekly-guidance` also needs `OPENAI_API_KEY` in the secret) |
| CloudWatch Logs | 90-day retention |
| Alarm + SNS topic `community-metrics-alerts` | Notifies the topic when any run fails |

## How it deploys

GitHub Actions (`.github/workflows/deploy.yml`) deploys on every merge to `main` that
touches the job code, `Dockerfile`, `uv.lock`/`pyproject.toml`, `src/dashboard/` or `infra/`,
and can be run by hand from the Actions tab. It runs the tests, builds both images on an
arm64 runner, and runs `cdk deploy --all`. Nobody needs Docker locally. A second job then
records the deploy under the repo's **Deployments** (`Production`) with a link to the
dashboard (`DASHBOARD_URL` in the workflow; change it there if the host's tailnet name
changes).

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

## Dashboard host

| Resource | Details |
| --- | --- |
| EC2 instance | `t4g.micro` (2 vCPU, 1 GB, arm64), Amazon Linux 2023 pinned by `dashboardAmi` in `cdk.json`, 16 GB encrypted gp3, IMDSv2 only with hop limit 1 (the container cannot reach the instance role's credentials), 1 GB swap, security updates installed daily (`dnf-automatic`) |
| Network | A public subnet in the account's default VPC (`dashboardVpcId` / `dashboardSubnetId` in `cdk.json`; us-east-2 is at its 5-VPC limit, so the stack creates no network). Its own security group has **no inbound rules**; Tailscale only makes outbound connections. No SSH (sshd is disabled and masked, and there is no key pair): use Session Manager (console → EC2 → Connect → Session Manager) |
| Dashboard container | Image built from `src/dashboard/Dockerfile`, published on `127.0.0.1:3000` only, `AUTH_MODE=tailscale`. Logs in CloudWatch (1-month retention) |
| Tailscale | Joins as `community-metrics` and runs `tailscale serve`, tailnet only (no Funnel). Now at https://community-metrics-1.taildd0df.ts.net (an older device held the plain name when it joined). Tagged `tag:community-metrics`, so it belongs to no person |
| Secrets Manager secret | `community-metrics/tailscale-auth-key`: a Tailscale auth key, filled in by hand. Kept if the stack is deleted. The LanceDB settings come from `community-metrics/prod` (only the `LANCEDB_*` keys reach the container) |
| SSM parameter | `/community-metrics/dashboard/image`: the image the host should run, set by each deploy |

**How it updates:** `cdk deploy` pushes the new image and updates the SSM parameter. On the
host, `dashboard-reconcile.timer` runs `/usr/local/bin/dashboard-reconcile`
(`community_metrics_infra/dashboard_host/dashboard-reconcile.sh`) every 2 minutes: it joins
the tailnet if needed and swaps the container when the parameter changes. GitHub never
connects to the host.

### One-time setup after the first deploy

1. Create a Tailscale auth key tagged `tag:community-metrics` (admin console → Settings →
   Keys → Generate auth key; one-off, pre-approved, with the tag). Only a tailnet admin, or
   someone the admins made an owner of that tag, can create it.
2. Paste it into the secret `community-metrics/tailscale-auth-key` (Secrets Manager console →
   Retrieve secret value → Edit → Plaintext, the bare `tskey-auth-...` value). Never in code,
   chat or shell history.
3. Within ~2 minutes the host joins the tailnet. Open
   `https://community-metrics.<tailnet>.ts.net` (or the `-1`, `-2` name Tailscale gives it if
   the plain name is taken). If the tailnet requires device approval, approve the new device
   in the admin console.

### Operating it

- Logs: CloudWatch log group from the stack (container output). Host script output:
  Session Manager → `journalctl -u dashboard-reconcile`.
- After rotating the LanceDB key in `community-metrics/prod`: Session Manager →
  `sudo docker rm -f dashboard && sudo systemctl start dashboard-reconcile` (a missing
  container is recreated with the current secret).
- Changing `dashboardAmi` or the host's user data **replaces the instance**: put a fresh
  auth key in the secret first, and remove the old device from the tailnet afterwards.
