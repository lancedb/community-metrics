#!/bin/bash
# Runs every 2 minutes on the dashboard host (systemd: dashboard-reconcile.timer).
# 1. Joins the tailnet once a Tailscale auth key is in the secret, and serves port 3000.
# 2. Runs the image named in the SSM parameter, replacing the container when it changes.
# Settings come from /etc/community-metrics/dashboard.env (written by the stack's user data).
# To pick up a rotated LanceDB key, remove the container (`docker rm -f dashboard`) and
# start the service; a missing container is recreated with the current secret.
set -euo pipefail

: "${AWS_REGION:?}" "${IMAGE_PARAMETER:?}" "${APP_SECRET_ID:?}" "${TAILSCALE_SECRET_ID:?}"
: "${TAILSCALE_HOSTNAME:?}" "${LOG_GROUP:?}"
export AWS_DEFAULT_REGION="$AWS_REGION"
RUNTIME_DIR=/run/community-metrics
CONTAINER=dashboard
umask 077
mkdir -p "$RUNTIME_DIR"

ensure_tailnet() {
  if tailscale status --json | jq -e '.BackendState == "Running"' >/dev/null 2>&1; then
    return 0
  fi
  local key_file="$RUNTIME_DIR/tailscale-auth-key"
  aws secretsmanager get-secret-value --secret-id "$TAILSCALE_SECRET_ID" \
    --query SecretString --output text > "$key_file"
  if ! grep -q '^tskey-' "$key_file"; then
    rm -f "$key_file"
    echo "Waiting for a Tailscale auth key in secret $TAILSCALE_SECRET_ID"
    return 1
  fi
  # file: keeps the key out of the process list.
  tailscale up --auth-key="file:$key_file" --hostname="$TAILSCALE_HOSTNAME"
  rm -f "$key_file"
  echo "Joined the tailnet as $TAILSCALE_HOSTNAME"
}

ensure_serve() {
  if tailscale serve status 2>/dev/null | grep -q 'proxy http://127.0.0.1:3000'; then
    return 0
  fi
  tailscale serve --bg 3000
}

ensure_container() {
  local image running
  image=$(aws ssm get-parameter --name "$IMAGE_PARAMETER" --query Parameter.Value --output text)
  running=$(docker inspect --format '{{.Config.Image}}' "$CONTAINER" 2>/dev/null || true)
  if [[ "$image" == "$running" ]] && docker ps -q --filter "name=^${CONTAINER}$" | grep -q .; then
    return 0
  fi

  echo "Deploying $image"
  aws ecr get-login-password | docker login --username AWS --password-stdin "${image%%/*}" >/dev/null
  docker pull --quiet "$image"

  # Only the LanceDB read settings reach the container; the other job keys stay in AWS.
  local env_file="$RUNTIME_DIR/app.env"
  aws secretsmanager get-secret-value --secret-id "$APP_SECRET_ID" --query SecretString --output text \
    | jq -r 'to_entries[] | select(.key | test("^LANCEDB_(API_KEY|HOST_OVERRIDE|REGION)$")) | "\(.key)=\(.value)"' \
    > "$env_file"

  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" --restart unless-stopped \
    --publish 127.0.0.1:3000:3000 \
    --env-file "$env_file" \
    --memory 700m \
    --log-driver awslogs --log-opt awslogs-group="$LOG_GROUP" --log-opt awslogs-region="$AWS_REGION" \
    "$image" >/dev/null
  rm -f "$env_file"

  for _ in $(seq 1 30); do
    if curl -fsS -o /dev/null http://127.0.0.1:3000/signin; then
      echo "Dashboard is up"
      docker image prune -af >/dev/null || true
      return 0
    fi
    sleep 2
  done
  echo "Dashboard did not answer on 127.0.0.1:3000 after 60s" >&2
  return 1
}

ensure_container
ensure_tailnet && ensure_serve
