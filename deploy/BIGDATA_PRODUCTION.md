# Big Data Production Integration

The public product runs through host Nginx:

- React: `https://mindschool.site/`
- FastAPI: `https://mindschool.site/api/...`
- Streamlit: `https://mindschool.site/dashboard/`

The current provider does not expose the host's standard public ports 80 and
443. Cloudflare Tunnel is therefore transport only:

```text
Internet -> Cloudflare Tunnel -> host Nginx :80 -> FE / BE / Dashboard
```

Host Nginx owns all application routing. Removing the tunnel requires the
provider to expose or forward public ports 80 and 443 to this host first.

The optional Big Data overlay adds:

- A single survey snapshot worker that writes
  `bronze/app_survey_snapshot/survey_all.parquet`.
- Backend chat events sent through a private SSH tunnel to the Kafka VM.
- GCS credentials for Backend and Dashboard.
- Dataproc Serverless Spark orchestration through Google Workflows and Cloud
  Scheduler.

## Required Secrets

Install these only on the production host:

```text
/opt/mindschool/app/deploy/secrets/gcp-service-account.json
/opt/mindschool/app/deploy/secrets/kafka-vm-ssh-key
```

The Kafka SSH key must authorize `Admin@34.21.211.62`. A GCP service account is
preferred. An `authorized_user` ADC file can be used as a temporary runtime
fallback when project IAM does not allow service-account key creation. The GCP
credential must be able to read/write the lake bucket. Workflow, Dataproc
Serverless, and Scheduler deployment still requires their corresponding GCP
permissions.

## Activate On The VPS

Keep `BIGDATA_ENABLED=false` until both secrets are installed. Then run:

```bash
cd /opt/mindschool/app
chown 10001:10001 deploy/secrets/gcp-service-account.json
chmod 400 deploy/secrets/gcp-service-account.json
chown root:root deploy/secrets/kafka-vm-ssh-key
chmod 600 deploy/secrets/kafka-vm-ssh-key
./deploy/deploy-bigdata.sh
```

The activation performs preflight checks, installs the persistent Kafka SSH
tunnel on `127.0.0.1:9092`, deploys the Big Data compose overlay, and verifies
the web routes plus Kafka and GCS Bronze/Silver/Gold data. If activation fails,
the script restores the core web deployment.

## Deploy Dataproc Automation

Run this from a trusted environment with `gcloud` installed:

```bash
cd /opt/mindschool/app
./deploy/gcp/deploy-bigdata-automation.sh --dry-run
./deploy/gcp/deploy-bigdata-automation.sh
```

The workflow uploads the Spark jobs and runs Survey Bronze-to-Silver,
Survey Silver-to-Gold core tables, Chat Bronze-to-Silver, and Chat
Silver-to-Gold sequentially. It stops if any Dataproc batch fails.

If project IAM does not allow deploying Google Workflows, install the VPS
systemd timer fallback. Spark still runs on Dataproc Serverless:

```bash
./deploy/gcp/run-bigdata-refresh.sh --dry-run
./deploy/install-bigdata-refresh-timer.sh
systemctl start mindschool-bigdata-refresh.service
```

The timer runs every day at `23:30 Asia/Ho_Chi_Minh`. After all four Dataproc
batches succeed, it updates the current Gold manifests and warms the persistent
Survey and Chat dashboard cache. The cache is stored in the Docker volume
`student-mental-health-platform_dashboard_cache` and survives Dashboard
container rebuilds and restarts.

## Verify

```bash
./deploy/verify-bigdata.sh
docker compose --env-file deploy/.env.production \
  -f deploy/docker-compose.production.yml \
  -f deploy/docker-compose.bigdata.yml ps
systemctl status mindschool-kafka-tunnel.service
```
