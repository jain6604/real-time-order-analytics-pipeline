# Zero-cost public deployment

## Selected architecture: honest public demo

The complete Kafka, Spark, MinIO, MongoDB, producer, and Streamlit architecture
runs locally with Docker Compose. It is intentionally **not** deployed as an
always-on public stack: a free Streamlit instance is limited to application
resources and sleeps after inactivity, so it is not a reliable host for Kafka
and Spark together.

The public portfolio deployment is a Streamlit Community Cloud dashboard using
the `dashboard/streamlit_demo.py` entrypoint. It renders deterministic sample
analytics and labels itself as a demo. It never claims to be a live stream.

## Cost and limits

- **Streamlit Community Cloud:** free public app hosting; the app can sleep
  after 12 hours without traffic and has shared resource limits. It serves the
  dashboard over HTTPS at a `streamlit.app` URL.
- **GitHub:** required as the source repository for Streamlit Community Cloud.
  Public-repository standard GitHub Actions runners are free. Private
  repositories have limited included minutes/storage; do not enable billing.
- **Remote streaming services:** none are provisioned. No cloud Kafka, Spark,
  MongoDB, MinIO, VM, storage volume, custom domain, or billing account is
  required for the public demo.

Official references: [Streamlit deployment](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app),
[Streamlit resource limits and hibernation](https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app),
and [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

## One-time manual publish steps

These actions require your GitHub and Streamlit accounts, so they cannot be
performed from this workspace without your explicit account authorization.

1. Create a **public** GitHub repository and push this project. Do not commit
   `.env` or `.streamlit/secrets.toml`.
2. Sign in at [Streamlit Community Cloud](https://share.streamlit.io/) with the
   GitHub account that owns the repository.
3. Choose **Create app**, select the repository and branch, and set the
   entrypoint to `dashboard/streamlit_demo.py`.
4. Deploy. Streamlit supplies a public HTTPS `streamlit.app` URL.

No Streamlit secrets are needed for the demo. Do not set a MongoDB URI for the
public app unless you intentionally redesign the deployment with a separately
secured database.

## Local live operation

Use the complete architecture locally:

```powershell
docker compose up --build -d
docker compose ps
docker compose logs -f spark producer
```

The live dashboard is available at `http://localhost:8501`; MinIO is local at
`http://localhost:9001`. Stop services without deleting persistent local data:

```powershell
docker compose stop
```

Do not use `docker compose down -v` unless you explicitly intend to remove
local Docker volumes.

## Updating the public demo

Push a reviewed change to the selected branch. Streamlit Community Cloud
redeploys from the repository. The GitHub Actions workflow validates tests,
Python compilation, Compose configuration, and the dashboard image first.
