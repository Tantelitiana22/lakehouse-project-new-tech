FROM nginx:1.27-alpine

COPY nginx.conf.template /etc/nginx/templates/default.conf.template

ENV GATEWAY_PORT=8088 \
    GATEWAY_BASE_DOMAIN=localhost

EXPOSE 8088

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD wget -q -O /dev/null "http://127.0.0.1:${GATEWAY_PORT}/healthz" || exit 1
