# ---------------------------------------------------------------------------
# Unity Catalog UI -- production image.
#
# Upstream's own `ui/Dockerfile` runs `react-scripts start` and bakes the API
# host into package.json at BUILD time via:
#
#     jq --arg proxy "http://$PROXY_HOST:8080" '.proxy = $proxy'
#
# That design does not survive contact with a real stack:
#
#   1. `proxy` is only honoured by the CRA *dev* server, so the image ships the
#      whole toolchain and recompiles the app on every container start.
#   2. The API host is frozen at build time, so one image is only correct for
#      one deployment topology.
#   3. react-scripts rejects unexpected Host headers with "Invalid Host header",
#      so the container answers 502 the moment it sits behind a proxy or is
#      addressed by anything other than the name baked in at build time.
#      (Verified: the published unitycatalog/unitycatalog-ui image proxies to
#      `http://server:8080`, which resolves to nothing in this project.)
#
# This build instead emits a real static bundle. The client already uses a
# *relative* axios baseURL -- `UC_API_PREFIX = '/api/2.1/unity-catalog'`, see
# upstream src/utils/constants.ts -- so nginx can serve the SPA and reverse
# proxy `/api/` to whichever Unity Catalog host is supplied at RUNTIME.
#
# The source is cloned from a pinned upstream tag rather than vendored, so this
# project carries no copy of the UI tree. Bump UC_UI_VERSION to move it.
# ---------------------------------------------------------------------------
FROM oven/bun:1.3.14-slim AS build

ARG UC_UI_VERSION=v0.7.0
ARG UC_REPO=https://github.com/unitycatalog/unitycatalog.git

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Only the UI subtree is needed; a sparse checkout keeps the clone small
# (the server, api specs and generated clients are tens of megabytes).
RUN git init -q \
    && git remote add origin "$UC_REPO" \
    && git fetch -q --depth 1 origin "refs/tags/${UC_UI_VERSION}:refs/tags/${UC_UI_VERSION}" \
    && git checkout -q "refs/tags/${UC_UI_VERSION}" -- ui

WORKDIR /src/ui

# Dependency manifests first, so the install layer survives source edits.
RUN bun install --frozen-lockfile

# CRA inlines REACT_APP_* at build time. All three auth providers default to
# off, which is what a self-hosted UC with no IdP wants. They are build args so
# an IdP-enabled image can be produced without editing this file.
ARG REACT_APP_GOOGLE_AUTH_ENABLED=false
ARG REACT_APP_OKTA_AUTH_ENABLED=false
ARG REACT_APP_KEYCLOAK_AUTH_ENABLED=false
ENV REACT_APP_GOOGLE_AUTH_ENABLED=${REACT_APP_GOOGLE_AUTH_ENABLED} \
    REACT_APP_OKTA_AUTH_ENABLED=${REACT_APP_OKTA_AUTH_ENABLED} \
    REACT_APP_KEYCLOAK_AUTH_ENABLED=${REACT_APP_KEYCLOAK_AUTH_ENABLED} \
    NODE_ENV=production

RUN bun run build


FROM nginx:1.27-alpine AS runtime

# nginx:alpine's entrypoint runs envsubst over /etc/nginx/templates/*.template
# against the container environment, so UC_SERVER and NGINX_PORT are resolved
# on startup and the same image works in any stack.
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build /src/ui/build /usr/share/nginx/html

# Must match the compose service name for `unity-catalog` in the target stack.
ENV UC_SERVER=http://unity-catalog:8080
ENV NGINX_PORT=3000

# 3000 matches upstream's UI port. The host port is chosen in compose, because
# 3000 is already taken by Dagster's webserver in this project.
EXPOSE 3000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD wget -q -O /dev/null "http://127.0.0.1:${NGINX_PORT}/" || exit 1
