"""Scheduled ingestion jobs: one container-image Lambda, invoked by EventBridge Scheduler.

The Lambda runs `community_metrics.lambda_handler.handler` from the repo-root Dockerfile.
Secret values are never in code: the secret is created with a placeholder and filled in
by hand (see infra/README.md).
"""

from __future__ import annotations

from pathlib import Path

import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_cloudwatch_actions as cw_actions
import aws_cdk.aws_ecr_assets as ecr_assets
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_logs as logs
import aws_cdk.aws_scheduler as scheduler
import aws_cdk.aws_scheduler_targets as targets
import aws_cdk.aws_secretsmanager as secretsmanager
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as subs
from aws_cdk import Duration, RemovalPolicy, Stack
from constructs import Construct

REPO_ROOT = Path(__file__).resolve().parents[2]

# name -> (cron fields, event payload). Times are UTC. Keep in sync with README.md.
SCHEDULES: dict[str, tuple[dict[str, str], dict[str, object]]] = {
    "daily": (
        {"minute": "0", "hour": "11"},
        {"job": "daily"},
    ),
    "weekly-guidance": (
        {"minute": "30", "hour": "11", "week_day": "MON"},
        {"job": "generate_signal_guidance", "args": ["--window-days", "7"]},
    ),
    "monthly-duckdb": (
        {"minute": "0", "hour": "12", "day": "1"},
        {"job": "update_duckdb_extension_downloads"},
    ),
}


class JobsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        secret_name: str,
        alert_emails: list[str],
        schedules_enabled: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Placeholder value; paste the real JSON in by hand after the first deploy.
        # RETAIN so a stack delete never destroys the credentials.
        secret = secretsmanager.Secret(
            self,
            "JobsSecret",
            secret_name=secret_name,
            description=(
                "community-metrics job settings as a JSON object of env vars "
                "(LANCEDB_API_KEY, LANCEDB_HOST_OVERRIDE, LANCEDB_REGION, "
                "GITHUB_TOKEN, OPENAI_API_KEY). Set by hand, never in code."
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )

        log_group = logs.LogGroup(
            self,
            "JobsLogs",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        fn = lambda_.DockerImageFunction(
            self,
            "JobsFunction",
            description="community-metrics scheduled ingestion jobs",
            code=lambda_.DockerImageCode.from_image_asset(
                str(REPO_ROOT),
                platform=ecr_assets.Platform.LINUX_ARM64,
            ),
            architecture=lambda_.Architecture.ARM_64,
            memory_size=1024,
            timeout=Duration.minutes(15),
            # One run at a time: schedules and retries never overlap.
            reserved_concurrent_executions=1,
            # Scheduler invokes asynchronously, so a failed run is retried here
            # (Scheduler's own retries only cover failed invoke calls). Jobs are
            # idempotent, so re-running the whole chain is safe.
            retry_attempts=2,
            max_event_age=Duration.hours(1),
            environment={"SECRET_ID": secret.secret_name},
            log_group=log_group,
        )
        secret.grant_read(fn)

        for name, (cron, payload) in SCHEDULES.items():
            scheduler.Schedule(
                self,
                f"Schedule-{name}",
                description=f"community-metrics {name}",
                schedule=scheduler.ScheduleExpression.cron(**cron),
                target=targets.LambdaInvoke(
                    fn,
                    input=scheduler.ScheduleTargetInput.from_object(payload),
                    retry_attempts=2,
                    max_event_age=Duration.hours(1),
                ),
                enabled=schedules_enabled,
            )

        # The handler raises when any step fails, so every failed run counts as an error.
        topic = sns.Topic(self, "AlertsTopic", display_name="community-metrics alerts")
        for email in alert_emails:
            topic.add_subscription(subs.EmailSubscription(email))

        alarm = cloudwatch.Alarm(
            self,
            "JobsErrorsAlarm",
            alarm_description="A community-metrics job failed. Check the Lambda logs.",
            metric=fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        alarm.add_alarm_action(cw_actions.SnsAction(topic))
