# DataFinch Web — Next.js 16 frontend
# Multi-stage Dockerfile. Defaults to dev mode for hot reload; switch to prod by
# overriding the CMD with `npm run build && npm start` or building target=prod.
FROM node:20-alpine AS base

WORKDIR /app

# Install OS deps needed for Next.js dev server (libc6-compat for some native
# modules; bash + curl for healthcheck convenience).
RUN apk add --no-cache libc6-compat curl

# Install Node deps first so Docker caches this layer when only source changes.
COPY package.json package-lock.json* ./
RUN npm install --no-audit --no-fund

# Copy source. In dev mode this is overlaid by a bind mount from the host
# (defined in docker-compose) — but we still need it baked in for the prod
# fallback path and for the first run before any mount kicks in.
COPY . .

ENV NEXT_TELEMETRY_DISABLED=1
# Polling-based file watcher — required on Docker-Desktop / Windows where
# native inotify events from the host bind mount don't reach the container.
ENV WATCHPACK_POLLING=true
ENV CHOKIDAR_USEPOLLING=true

EXPOSE 3000

# Dev mode by default. For production:
#   docker compose run datafinch-web sh -c "npm run build && npm start"
CMD ["npm", "run", "dev"]
