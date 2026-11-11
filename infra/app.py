"""CDK app for the community-metrics scheduled jobs. See infra/README.md."""

import os

import aws_cdk as cdk
from community_metrics_infra.jobs_stack import JobsStack

app = cdk.App()


def _context(name: str, default: str = "") -> str:
    return str(app.node.try_get_context(name) or default).strip()


def _schedule_names(value: object) -> list[str]:
    # A list in cdk.json, or a comma-separated string from `cdk deploy -c`.
    if isinstance(value, str):
        value = value.split(",")
    return [str(v).strip() for v in value or [] if str(v).strip()]


stack = JobsStack(
    app,
    "CommunityMetricsJobs",
    secret_name=_context("secretName", "community-metrics/prod"),
    # Names from SCHEDULES that run, set in cdk.json. The rest are created DISABLED.
    enabled_schedules=_schedule_names(app.node.try_get_context("enabledSchedules")),
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        # Same region as devrel-proxy, which the jobs use to reach LanceDB.
        region=_context("region", "us-east-2"),
    ),
)
cdk.Tags.of(stack).add("Project", "community-metrics")

app.synth()
