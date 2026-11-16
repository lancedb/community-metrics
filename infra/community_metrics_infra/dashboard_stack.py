"""The dashboard on one small EC2 instance, reachable only over the LanceDB tailnet.

Nothing is open to the internet: the security group has no inbound rules. The instance
runs Tailscale, which makes outbound connections only, and `tailscale serve` publishes
the dashboard container (bound to 127.0.0.1) at https://<tailscale_hostname>.<tailnet>.ts.net.
Admin access is through AWS Systems Manager Session Manager, not SSH.

Deploys are pull-based. `cdk deploy` builds the dashboard image (src/dashboard/Dockerfile),
pushes it to the CDK assets repository and writes its URI to an SSM parameter. A systemd
timer on the instance runs `dashboard-reconcile` every 2 minutes: it joins the tailnet if
needed, and replaces the container when the parameter changes. GitHub never needs access
to the instance.

Secret values are never in code: the Tailscale auth key secret is created with a
placeholder and filled in by hand (see infra/README.md).
"""

from __future__ import annotations

from pathlib import Path

import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_ecr_assets as ecr_assets
import aws_cdk.aws_iam as iam
import aws_cdk.aws_logs as logs
import aws_cdk.aws_secretsmanager as secretsmanager
import aws_cdk.aws_ssm as ssm
from aws_cdk import CfnOutput, RemovalPolicy, Stack
from constructs import Construct

REPO_ROOT = Path(__file__).resolve().parents[2]
RECONCILE_SCRIPT = (
    Path(__file__).resolve().parent / "dashboard_host" / "dashboard-reconcile.sh"
)

IMAGE_PARAMETER_NAME = "/community-metrics/dashboard/image"


class DashboardStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        app_secret_name: str,
        tailscale_secret_name: str,
        ami_id: str,
        tailscale_hostname: str = "community-metrics",
        instance_type: str = "t4g.micro",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if not ami_id:
            raise ValueError(
                "dashboardAmi must be set in cdk.json (an Amazon Linux 2023 arm64 AMI)"
            )

        # Own small VPC: one public subnet for outbound traffic (Tailscale, ECR, AWS APIs),
        # no NAT gateway. Inbound is closed by the security group below.
        vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.60.0.0/24"),
            max_azs=1,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=26
                )
            ],
        )

        security_group = ec2.SecurityGroup(
            self,
            "HostSecurityGroup",
            vpc=vpc,
            description="community-metrics dashboard host: no inbound; outbound for Tailscale, ECR and AWS APIs",
            allow_all_outbound=True,
        )

        image = ecr_assets.DockerImageAsset(
            self,
            "DashboardImage",
            directory=str(REPO_ROOT / "src" / "dashboard"),
            platform=ecr_assets.Platform.LINUX_ARM64,
        )

        image_parameter = ssm.StringParameter(
            self,
            "DashboardImageParameter",
            parameter_name=IMAGE_PARAMETER_NAME,
            description="Dashboard image the host should run; updated by every cdk deploy",
            string_value=image.image_uri,
        )

        # Placeholder value; paste a Tailscale auth key in by hand. RETAIN so a stack delete
        # never destroys it.
        tailscale_secret = secretsmanager.Secret(
            self,
            "TailscaleAuthKey",
            secret_name=tailscale_secret_name,
            description=(
                "Tailscale auth key (tskey-auth-...) the dashboard host uses to join the tailnet. "
                "Set by hand, never in code."
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        # Shared with the jobs; the host passes only the LANCEDB_* keys to the container.
        app_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "AppSecret", app_secret_name
        )

        log_group = logs.LogGroup(
            self,
            "DashboardLogs",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        role = iam.Role(
            self,
            "HostRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="community-metrics dashboard host",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                )
            ],
        )
        image.repository.grant_pull(role)
        image_parameter.grant_read(role)
        tailscale_secret.grant_read(role)
        app_secret.grant_read(role)
        log_group.grant_write(role)

        settings = {
            "AWS_REGION": self.region,
            "IMAGE_PARAMETER": image_parameter.parameter_name,
            "APP_SECRET_ID": app_secret_name,
            "TAILSCALE_SECRET_ID": tailscale_secret.secret_name,
            "TAILSCALE_HOSTNAME": tailscale_hostname,
            "LOG_GROUP": log_group.log_group_name,
        }
        user_data = ec2.UserData.for_linux()
        user_data.add_commands(*_bootstrap_commands(settings))

        instance = ec2.Instance(
            self,
            "Host",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            associate_public_ip_address=True,
            security_group=security_group,
            instance_type=ec2.InstanceType(instance_type),
            machine_image=ec2.MachineImage.generic_linux({self.region: ami_id}),
            role=role,
            user_data=user_data,
            # User data only runs on first boot, so a change must replace the instance.
            # A replacement needs a fresh Tailscale auth key (see infra/README.md).
            user_data_causes_replacement=True,
            require_imdsv2=True,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        16, encrypted=True, volume_type=ec2.EbsDeviceVolumeType.GP3
                    ),
                )
            ],
        )

        CfnOutput(self, "InstanceId", value=instance.instance_id)
        CfnOutput(self, "TailscaleSecretName", value=tailscale_secret.secret_name)
        CfnOutput(
            self,
            "DashboardUrlHint",
            value=f"https://{tailscale_hostname}.<tailnet>.ts.net",
        )


def _bootstrap_commands(settings: dict[str, str]) -> list[str]:
    env_lines = "\n".join(f"{key}={value}" for key, value in settings.items())
    script = RECONCILE_SCRIPT.read_text()
    return [
        "set -euxo pipefail",
        # Packages: Docker, jq, Tailscale, and automatic security updates.
        "dnf install -y docker jq dnf-automatic",
        "dnf config-manager --add-repo https://pkgs.tailscale.com/stable/amazon-linux/2023/tailscale.repo",
        "dnf install -y tailscale",
        "systemctl enable --now docker tailscaled",
        "sed -i 's/^upgrade_type.*/upgrade_type = security/' /etc/dnf/automatic.conf",
        "systemctl enable --now dnf-automatic-install.timer",
        # 1 GB swap: the instance has 1 GB of memory.
        (
            "if [ ! -f /swapfile ]; then fallocate -l 1G /swapfile && chmod 600 /swapfile"
            " && mkswap /swapfile && swapon /swapfile && echo '/swapfile none swap sw 0 0' >> /etc/fstab; fi"
        ),
        # Settings (names only, no secret values) and the reconcile script.
        "mkdir -p /etc/community-metrics",
        f"cat > /etc/community-metrics/dashboard.env <<'EOF_SETTINGS'\n{env_lines}\nEOF_SETTINGS",
        f"cat > /usr/local/bin/dashboard-reconcile <<'EOF_SCRIPT'\n{script}EOF_SCRIPT",
        "chmod 755 /usr/local/bin/dashboard-reconcile",
        (
            "cat > /etc/systemd/system/dashboard-reconcile.service <<'EOF_UNIT'\n"
            "[Unit]\n"
            "Description=Join the tailnet and run the current community-metrics dashboard image\n"
            "Wants=network-online.target docker.service tailscaled.service\n"
            "After=network-online.target docker.service tailscaled.service\n"
            "[Service]\n"
            "Type=oneshot\n"
            "EnvironmentFile=/etc/community-metrics/dashboard.env\n"
            "ExecStart=/usr/local/bin/dashboard-reconcile\n"
            "EOF_UNIT"
        ),
        (
            "cat > /etc/systemd/system/dashboard-reconcile.timer <<'EOF_UNIT'\n"
            "[Unit]\n"
            "Description=Check for a new dashboard image every 2 minutes\n"
            "[Timer]\n"
            "OnBootSec=30s\n"
            "OnUnitActiveSec=2min\n"
            "[Install]\n"
            "WantedBy=timers.target\n"
            "EOF_UNIT"
        ),
        "systemctl daemon-reload",
        "systemctl enable --now dashboard-reconcile.timer",
    ]
