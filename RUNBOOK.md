# Runbook

> [!IMPORTANT]
> **Retired on 2026-11-27.** Community metrics are now part of
> [lance-overwatch](https://github.com/lancedb/lance-overwatch): a daily GitHub Actions job
> collects the same downloads, stars and DuckDB numbers, and its Adoption page
> (https://lance-overwatch.taildd0df.ts.net/#/adoption, LanceDB tailnet only) shows them with
> the Insights tab. The full table history was imported there first.
>
> The AWS stacks (`CommunityMetricsJobs`, `CommunityMetricsDashboard`), the dashboard host, the
> LanceDB tables and the secrets are deleted, and the `Deploy` workflow is removed so a merge
> can't recreate them. The code is kept for reference.

How to run community metrics day to day. Read this first if you are taking it over.
Setup details live in [README.md](README.md) (what it tracks, the jobs, the dashboard) and
[infra/README.md](infra/README.md) (the AWS resources). This repo is public: never put a key,
password or email address in it, in a PR or in an issue.

## What it is

```
EventBridge Scheduler ──► Lambda (jobs) ──┐
                                          ├──► devrel-proxy ──► LanceDB Enterprise cluster
tailnet user ──► EC2 host (dashboard) ────┘
```

| Piece | Where | What it does |
| --- | --- | --- |
| Jobs | Lambda in stack `CommunityMetricsJobs` (AWS account `054483968661`, us-east-2) | Fetches downloads and stars every day and writes them to LanceDB. Monthly: DuckDB extension downloads |
| Dashboard | EC2 `t4g.micro` in stack `CommunityMetricsDashboard`, same account and region | Serves https://community-metrics-1.taildd0df.ts.net to `@lancedb.com` users on the LanceDB tailnet. No inbound ports, no SSH |
| Data | LanceDB Enterprise cluster, shared with the DevRel demos | Tables `metrics`, `stats`, `history` (source of truth) and the derived dashboard tables |
| devrel-proxy | EC2 `i-0aee7280806739d37`, owned by the DevRel/AWS admins, not by this repo | Passes requests from outside AWS to the cluster's internal load balancer. The demos use it too: never stop it |
| Secrets | Secrets Manager `community-metrics/prod` (LanceDB, GitHub, OpenAI keys) and `community-metrics/tailscale-auth-key` | Filled in by hand, never in code |
| Deploys | GitHub Actions `.github/workflows/deploy.yml` | Every merge to `main` that touches code or `infra/` runs the tests and `cdk deploy --all` |
| Alerts | SNS topic `community-metrics-alerts` | Email when a job run fails |

## Access you need

- **AWS:** sign-in to account `054483968661` (us-east-2) with rights to Lambda, EC2,
  Secrets Manager, CloudWatch and Systems Manager. Ask the AWS admins.
- **GitHub:** write access to this repo, to open PRs. Merging to `main` deploys.
- **Tailnet:** a LanceDB tailnet account, to open the dashboard.
- **Alerts:** subscribe your email to `community-metrics-alerts` (SNS console → Topics →
  `community-metrics-alerts` → Create subscription → Email, then confirm the email).
- **Who to ask:** AWS account and devrel-proxy: the AWS/DevRel admins. LanceDB cluster and
  API keys: the cluster owner. Tailnet tags, keys and devices: a tailnet admin.

## Routine checks

Nothing needs doing day to day. Once a week, or when an alert arrives:

1. Open the dashboard and check that the latest day in the 30-day tables is yesterday or
   today.
2. Lambda console → the `CommunityMetricsJobs-JobsFunction…` function → Monitor: one run a
   day, no errors.
3. GitHub → Actions: the latest `Deploy` and `Dashboard UI tests` runs on `main` passed.

## Changing code

1. Branch, change, open a PR. CI runs the dashboard UI tests on dashboard changes.
2. Run the checks locally first: `uv run pytest`, `uv run ruff check .`,
   `cd src/dashboard && npm run test:ui && npm run typecheck`, `cd infra && uv run pytest`.
3. Merge. `Deploy` runs automatically (about 10 minutes). The dashboard host picks up a new
   image within 2 minutes after that.
4. If `Deploy` fails, read its log in GitHub Actions, fix forward with another PR, and
   re-run it from the Actions tab (Run workflow) if needed.

## A job run failed

You get an alert email, or the dashboard's latest day falls behind.

1. **Find the error:** CloudWatch → Log groups → `CommunityMetricsJobs-JobsLogs…`, newest
   stream. Or check the `history` table: `uv run debug.py history --start-date <date>`.
2. **Common causes:**
   - *A key expired or was revoked* (GitHub 401, LanceDB 401/403): put a new key in the
     secret (see [Rotating a key](#rotating-a-key)). No redeploy needed.
   - *A registry outage* (PyPI, npm, crates.io 5xx or 429): nothing to do. The next daily run
     retries missed days.
   - *devrel-proxy or the cluster is down* (connection errors to `LANCEDB_HOST_OVERRIDE`): ask
     the devrel-proxy or cluster owners.
3. **Re-run it:** Lambda console → the function → Test → event `{"job": "daily"}` → Test.
4. **Fill a longer gap:** run `{"job": "daily_refresh", "args": ["--lookback-days", "14"]}`,
   then `{"job": "derive_dashboard"}`. Registries keep daily download counts for a limited
   time (crates.io about 90 days), so don't wait months.

Missed **star** days can't be refilled by the Lambda: rebuilding past star days needs
`update_daily_stars --lookback-days N` run locally with a GitHub token that has write access
on the repos (see README).

Only the jobs in `ALLOWED_JOBS` (`src/community_metrics/lambda_handler.py`) run on Lambda.
Destructive jobs (`bootstrap_tables`, `update_all`, `seed_from_csv`) are deliberately
excluded: run those locally, and only if you mean to rebuild the tables.

## The dashboard is down or shows errors

1. **Is the host up?** EC2 → Instances → the `CommunityMetricsDashboard` instance → status
   checks. Is it online in the tailnet admin console?
2. **Open a shell:** EC2 → the instance → Connect → Session Manager → Connect (there is no
   SSH). Then:
   ```bash
   sudo docker ps                                   # is the dashboard container running?
   sudo journalctl -u dashboard-reconcile -n 50     # what the update script did
   tailscale status && tailscale serve status       # on the tailnet? serving port 3000?
   curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3000/signin   # 200 = app is up
   ```
3. **App errors** (charts empty, 500s from `/api/v1/dashboard/daily`): CloudWatch → Log groups
   → `CommunityMetricsDashboard-DashboardLogs…`. Usually the LanceDB key or devrel-proxy.
4. **Restart the container:** `sudo docker rm -f dashboard && sudo systemctl start dashboard-reconcile`.
   It is recreated with the current image and secret.
5. **Reboot** if the host is stuck: EC2 → Instance state → Reboot. Everything restarts on its
   own.

Data from a new job run can take up to 5 minutes to show (the API caches responses).

## Rotating a key

1. Create the new key at the provider (LanceDB cluster owner, GitHub, OpenAI).
2. Secrets Manager → `community-metrics/prod` → Retrieve secret value → Edit → change only that
   value → Save. Never paste keys into chat, code, PRs or shell history.
3. **Jobs:** the next run picks it up. To check now, run `{"job": "daily"}` from the Lambda
   console.
4. **Dashboard** (LanceDB keys only): Session Manager on the host →
   `sudo docker rm -f dashboard && sudo systemctl start dashboard-reconcile`.
5. Revoke the old key at the provider once both work.

If the devrel-proxy password changes, the new `LANCEDB_HOST_OVERRIDE` URL goes in the same
secret, with the same steps.

## Rebuilding or replacing the dashboard host

Changing `dashboardAmi` in `infra/cdk.json`, or the host's user data in
`infra/community_metrics_infra/dashboard_stack.py`, replaces the instance on the next deploy.
The new instance needs a Tailscale key to join the tailnet:

1. **Before merging:** get a one-off, pre-approved auth key tagged `tag:community-metrics`
   from a tailnet admin (or create it yourself if you are an owner of that tag). Put the bare
   `tskey-auth-…` value in Secrets Manager → `community-metrics/tailscale-auth-key`.
2. Merge. After `Deploy`, the new host joins within a few minutes.
3. In the tailnet admin console, remove the old device. If the new one got a `-1`/`-2` name,
   rename it (Session Manager → `sudo tailscale set --hostname=community-metrics`) and update
   the URL in this file and README.md.

## Keeping it secure

These rules are tested in `infra/tests/` and listed in `AGENTS.md`. Don't weaken them:

- The dashboard host has no inbound security-group rules, IMDSv2 with hop limit 1, sshd
  disabled, and the container bound to `127.0.0.1`.
- `AUTH_MODE=tailscale` trusts the `Tailscale-User-Login` header, which is only safe because
  `tailscale serve` is the only way in. Never use it on anything reachable another way, and
  never run `tailscale funnel` on the host.
- Secrets live only in Secrets Manager. The host passes only the `LANCEDB_*` keys to the
  container.
- Security updates install automatically on the host (`dnf-automatic`). Dependency updates
  (`uv.lock`, `src/dashboard/package-lock.json`) go through normal PRs: check them with
  `uvx pip-audit` and `npm audit` now and then.

## Costs

About $12/month: the `t4g.micro` host (~$6), its public IPv4 address (~$3.60) and disk
(~$1.30), Secrets Manager (~$0.80), and Lambda, logs and alerts (cents). devrel-proxy is
billed separately to its owners.
