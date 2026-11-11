import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template
from community_metrics_infra.jobs_stack import SCHEDULES, JobsStack


def _template(**overrides) -> Template:
    app = cdk.App()
    kwargs = {
        "secret_name": "community-metrics/test",
        "schedules_enabled": True,
    } | overrides
    return Template.from_stack(JobsStack(app, "Test", **kwargs))


@pytest.fixture(scope="module")
def template() -> Template:
    return _template()


def test_lambda_shape(template):
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "PackageType": "Image",
            "Architectures": ["arm64"],
            "MemorySize": 1024,
            "Timeout": 900,
            "ReservedConcurrentExecutions": 1,
            "Environment": {"Variables": {"SECRET_ID": Match.any_value()}},
        },
    )
    template.has_resource_properties(
        "AWS::Lambda::EventInvokeConfig",
        {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600},
    )


def test_secret_is_retained_and_has_no_real_value(template):
    template.has_resource(
        "AWS::SecretsManager::Secret",
        {
            "DeletionPolicy": "Retain",
            "Properties": {
                "Name": "community-metrics/test",
                "SecretString": Match.absent(),
            },
        },
    )


def test_function_can_read_only_its_secret(template):
    statements = [
        s
        for policy in template.find_resources("AWS::IAM::Policy").values()
        for s in policy["Properties"]["PolicyDocument"]["Statement"]
        if any("secretsmanager" in a for a in _as_list(s["Action"]))
    ]
    assert statements
    for s in statements:
        assert set(_as_list(s["Action"])) <= {
            "secretsmanager:GetSecretValue",
            "secretsmanager:DescribeSecret",
        }
        assert s["Resource"] != "*"


def test_schedules_send_the_handler_payloads(template):
    schedules = template.find_resources("AWS::Scheduler::Schedule")
    assert len(schedules) == len(SCHEDULES)
    inputs = sorted(
        json.loads(s["Properties"]["Target"]["Input"])["job"]
        for s in schedules.values()
    )
    assert inputs == sorted(p["job"] for _, p in SCHEDULES.values())
    for s in schedules.values():
        assert s["Properties"]["State"] == "ENABLED"
        assert s["Properties"]["Target"]["RetryPolicy"]["MaximumRetryAttempts"] == 2


def test_schedules_can_be_created_disabled():
    schedules = _template(schedules_enabled=False).find_resources(
        "AWS::Scheduler::Schedule"
    )
    assert {s["Properties"]["State"] for s in schedules.values()} == {"DISABLED"}


def test_errors_alarm_notifies_topic_without_hardcoded_emails(template):
    template.has_resource_properties(
        "AWS::CloudWatch::Alarm",
        {"MetricName": "Errors", "Threshold": 1, "AlarmActions": Match.any_value()},
    )
    template.has_resource_properties(
        "AWS::SNS::Topic", {"TopicName": "community-metrics-alerts"}
    )
    template.resource_count_is("AWS::SNS::Subscription", 0)


def test_logs_kept_90_days(template):
    template.has_resource_properties("AWS::Logs::LogGroup", {"RetentionInDays": 90})


def _as_list(value):
    return value if isinstance(value, list) else [value]


def test_cdk_json_keeps_schedules_off_by_default():
    import json
    from pathlib import Path

    cdk_json = json.loads(
        (Path(__file__).resolve().parents[1] / "cdk.json").read_text()
    )
    assert cdk_json["context"]["schedulesEnabled"] is False
