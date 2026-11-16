"""CDK app for community-metrics: the scheduled jobs and the tailnet dashboard host. See infra/README.md."""

import os

import aws_cdk as cdk
from community_metrics_infra.dashboard_stack import DashboardStack
from community_metrics_infra.jobs_stack import JobsStack

app = cdk.App()


def _context(name: str, default: str = "") -> str:
    return str(app.node.try_get_context(name) or default).strip()


def _schedule_names(value: object) -> list[str]:
    # A list in cdk.json, or a comma-separated string from `cdk deploy -c`.
    if isinstance(value, str):
        value = value.split(",")
    return [str(v).strip() for v in value or [] if str(v).strip()]


env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    # Same region as devrel-proxy, which the jobs and the dashboard use to reach LanceDB.
    region=_context("region", "us-east-2"),
)
secret_name = _context("secretName", "community-metrics/prod")

jobs = JobsStack(
    app,
    "CommunityMetricsJobs",
    secret_name=secret_name,
    # Names from SCHEDULES that run, set in cdk.json. The rest are created DISABLED.
    enabled_schedules=_schedule_names(app.node.try_get_context("enabledSchedules")),
    env=env,
)

dashboard = DashboardStack(
    app,
    "CommunityMetricsDashboard",
    # The jobs stack creates this secret; the dashboard reads its LANCEDB_* keys.
    app_secret_name=secret_name,
    tailscale_secret_name=_context(
        "tailscaleSecretName", "community-metrics/tailscale-auth-key"
    ),
    # Pinned Amazon Linux 2023 arm64 AMI. Changing it replaces the instance (needs a new
    # Tailscale auth key); security updates install automatically in between.
    ami_id=_context("dashboardAmi"),
    tailscale_hostname=_context("tailscaleHostname", "community-metrics"),
    env=env,
)
dashboard.add_stack_dependency(jobs)

for stack in (jobs, dashboard):
    cdk.Tags.of(stack).add("Project", "community-metrics")

app.synth()
