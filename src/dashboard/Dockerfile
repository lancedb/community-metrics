# Container image for the dashboard on the tailnet host (infra/community_metrics_infra/dashboard_stack.py).
# It builds Next.js in standalone mode and runs `node server.js`. Secrets are passed in at
# run time by the host (LANCEDB_* only), never baked into the image.
#
# The host publishes the port on 127.0.0.1 only; `tailscale serve` is the single way in,
# which is what makes AUTH_MODE=tailscale safe (see src/lib/auth-policy.ts).

FROM node:22-bookworm-slim AS build
# Next.js traces files from the repo root (outputFileTracingRoot = ../..), so keep the
# same layout inside the image.
WORKDIR /repo/src/dashboard
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ENV NEXT_OUTPUT_STANDALONE=1 NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM node:22-bookworm-slim
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 AUTH_MODE=tailscale HOSTNAME=0.0.0.0 PORT=3000
WORKDIR /app
COPY --from=build --chown=node:node /repo/src/dashboard/.next/standalone ./
COPY --from=build --chown=node:node /repo/src/dashboard/.next/static ./src/dashboard/.next/static
COPY --from=build --chown=node:node /repo/src/dashboard/public ./src/dashboard/public
USER node
EXPOSE 3000
CMD ["node", "src/dashboard/server.js"]
