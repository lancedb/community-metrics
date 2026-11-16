import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template
from community_metrics_infra.dashboard_stack import DashboardStack


@pytest.fixture(scope="module")
def template() -> Template:
    app = cdk.App()
    stack = DashboardStack(
        app,
        "Test",
        app_secret_name="community-metrics/test",
        tailscale_secret_name="community-metrics/test-tailscale",
        ami_id="ami-0123456789abcdef0",
        vpc_id="vpc-0123456789abcdef0",
        subnet_id="subnet-0123456789abcdef0",
        availability_zone="us-east-2a",
        env=cdk.Environment(account="111111111111", region="us-east-2"),
    )
    return Template.from_stack(stack)


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _user_data(template: Template) -> str:
    (instance,) = template.find_resources("AWS::EC2::Instance").values()
    parts = instance["Properties"]["UserData"]["Fn::Base64"]
    if isinstance(parts, str):
        return parts
    return "".join(
        p if isinstance(p, str) else json.dumps(p) for p in parts["Fn::Join"][1]
    )


def test_nothing_is_open_to_inbound_traffic(template):
    for sg in template.find_resources("AWS::EC2::SecurityGroup").values():
        assert not sg["Properties"].get("SecurityGroupIngress")
    assert not template.find_resources("AWS::EC2::SecurityGroupIngress")
    assert not template.find_resources("AWS::EC2::NatGateway")


def test_uses_the_given_subnet_and_creates_no_network(template):
    # The region is at its VPC limit: the host must go into an existing public subnet.
    for kind in ("AWS::EC2::VPC", "AWS::EC2::InternetGateway", "AWS::EC2::Subnet"):
        assert not template.find_resources(kind)
    template.has_resource_properties(
        "AWS::EC2::Instance",
        {
            "AvailabilityZone": "us-east-2a",
            "NetworkInterfaces": [
                Match.object_like(
                    {
                        "SubnetId": "subnet-0123456789abcdef0",
                        "AssociatePublicIpAddress": True,
                    }
                )
            ],
        },
    )


def test_instance_shape(template):
    template.has_resource_properties(
        "AWS::EC2::Instance",
        {
            "InstanceType": "t4g.micro",
            "ImageId": "ami-0123456789abcdef0",
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {"Encrypted": True, "VolumeSize": 16, "VolumeType": "gp3"},
                }
            ],
        },
    )
    template.has_resource_properties(
        "AWS::EC2::LaunchTemplate",
        {"LaunchTemplateData": {"MetadataOptions": {"HttpTokens": "required"}}},
    )


def test_container_cannot_reach_instance_metadata(template):
    # Regression (security audit 2026-11-16): the AL2023 AMI default hop limit of 2 let
    # the dashboard container fetch the instance role's credentials. IMDSv2 plus hop
    # limit 1 keeps the metadata service reachable from the host only.
    template.has_resource_properties(
        "AWS::EC2::LaunchTemplate",
        {
            "LaunchTemplateData": {
                "MetadataOptions": {
                    "HttpTokens": "required",
                    "HttpPutResponseHopLimit": 1,
                }
            }
        },
    )


def test_tailscale_secret_is_retained_and_has_no_real_value(template):
    template.has_resource(
        "AWS::SecretsManager::Secret",
        {
            "DeletionPolicy": "Retain",
            "Properties": {
                "Name": "community-metrics/test-tailscale",
                "SecretString": Match.absent(),
            },
        },
    )


def test_host_permissions_are_scoped(template):
    statements = [
        s
        for policy in template.find_resources("AWS::IAM::Policy").values()
        for s in policy["Properties"]["PolicyDocument"]["Statement"]
    ]
    actions = {a for s in statements for a in _as_list(s["Action"])}
    assert "secretsmanager:GetSecretValue" in actions
    assert "ssm:GetParameter" in actions
    assert "ecr:BatchGetImage" in actions
    # No write access to secrets, parameters or images.
    assert not {
        a
        for a in actions
        if a.startswith(
            ("secretsmanager:Put", "secretsmanager:Update", "ssm:Put", "ecr:Put")
        )
    }
    for s in statements:
        if s["Resource"] == "*":
            # Only the ECR login call cannot be scoped to a resource.
            assert _as_list(s["Action"]) == ["ecr:GetAuthorizationToken"]


def test_user_data_installs_tailscale_and_never_holds_secret_values(template):
    user_data = _user_data(template)
    assert "dnf install -y tailscale" in user_data
    assert "dashboard-reconcile.timer" in user_data
    assert "tskey-auth" not in user_data
    assert "LANCEDB_API_KEY=" not in user_data
    assert "--publish 127.0.0.1:3000:3000" in user_data


def test_ssh_is_turned_off(template):
    # Regression (security audit 2026-11-16): sshd listened on port 22 and anyone on the
    # tailnet could reach it. Admin access is through Session Manager only.
    user_data = _user_data(template)
    assert "systemctl disable --now sshd" in user_data
    assert "systemctl mask sshd" in user_data
    # Never re-enabled later in the script.
    assert "enable --now sshd" not in user_data
    assert "start sshd" not in user_data
    # And the stack never adds an SSH key pair.
    (instance,) = template.find_resources("AWS::EC2::Instance").values()
    assert "KeyName" not in instance["Properties"]
