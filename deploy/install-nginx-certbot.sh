#!/usr/bin/env bash
set -euo pipefail

APP_DOMAIN="${APP_DOMAIN:-mindschool.site}"
ACME_EMAIL="${ACME_EMAIL:-admin@mindschool.site}"
APP_ROOT="${APP_ROOT:-/opt/mindschool/app}"
NGINX_AVAILABLE="/etc/nginx/conf.d/${APP_DOMAIN}.conf"
HTTP_ONLY_CONF="${APP_ROOT}/deploy/nginx/conf.d/${APP_DOMAIN}.http-only.conf"
HTTPS_CONF="${APP_ROOT}/deploy/nginx/conf.d/${APP_DOMAIN}.conf"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run this script as root." >&2
  exit 1
fi

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates \
  certbot \
  curl \
  docker.io \
  git \
  nginx \
  python3-certbot-nginx \
  ufw

if apt-cache show docker-compose-v2 >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose-v2
elif apt-cache show docker-compose >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose
fi

if ! docker compose version >/dev/null 2>&1; then
  mkdir -p /usr/local/lib/docker/cli-plugins
  case "$(uname -m)" in
    x86_64) compose_arch="x86_64" ;;
    aarch64|arm64) compose_arch="aarch64" ;;
    *) echo "Unsupported CPU architecture for Docker Compose: $(uname -m)" >&2; exit 1 ;;
  esac
  curl -fsSL \
    "https://github.com/docker/compose/releases/download/v2.33.1/docker-compose-linux-${compose_arch}" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

mkdir -p /opt/mindschool /var/www/certbot/.well-known/acme-challenge /etc/nginx/conf.d
systemctl enable --now docker
systemctl enable --now nginx

if command -v ufw >/dev/null 2>&1; then
  ufw allow OpenSSH || true
  ufw allow 80/tcp || true
  ufw allow 443/tcp || true
fi

cp "$HTTP_ONLY_CONF" "$NGINX_AVAILABLE"
nginx -t
systemctl reload nginx

challenge_token="mindschool-preflight-$(date +%s)-$$"
challenge_file="/var/www/certbot/.well-known/acme-challenge/${challenge_token}"
printf '%s\n' "$challenge_token" > "$challenge_file"

for domain in "$APP_DOMAIN" "www.${APP_DOMAIN}"; do
  if ! curl -fsS --max-time 20 "http://${domain}/.well-known/acme-challenge/${challenge_token}" \
    | grep -Fxq "$challenge_token"; then
    rm -f "$challenge_file"
    echo "ACME preflight failed for ${domain}. Point DNS to this host and open inbound port 80 before retrying." >&2
    exit 1
  fi
done
rm -f "$challenge_file"

certbot certonly \
  --webroot \
  --webroot-path /var/www/certbot \
  --domain "$APP_DOMAIN" \
  --domain "www.${APP_DOMAIN}" \
  --email "$ACME_EMAIL" \
  --agree-tos \
  --no-eff-email \
  --non-interactive

cp "$HTTPS_CONF" "$NGINX_AVAILABLE"
nginx -t
systemctl reload nginx

mkdir -p /etc/letsencrypt/renewal-hooks/deploy
cat > /etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh <<'EOF'
#!/usr/bin/env sh
set -eu
nginx -t
systemctl reload nginx
EOF
chmod +x /etc/letsencrypt/renewal-hooks/deploy/reload-nginx.sh

systemctl enable certbot.timer

echo "Nginx and Certbot are configured for ${APP_DOMAIN}."
