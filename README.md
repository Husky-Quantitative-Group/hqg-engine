# HQG Execution Engine

Run dashboard-created strategies against real Alpaca paper trading using
`compose.yaml` and `.env`. The stack includes PostgreSQL, the provider, isolated
strategy workers, and local AWS substitutes for the dashboard.

For a new setup, copy `.env.example` to `.env`. If `.env` already exists, add the
settings from the example while keeping your other values. Set `ALPACA_API_KEY`
and `ALPACA_SECRET_KEY` to your paper account keys and `ALPACA_PAPER=true`, then
configure the existing PostgreSQL settings (`DATABASE_URL` uses host `db`). Keep
the existing `hqg_network` Docker network, or create it for a new setup:

```sh
docker network create hqg_network # Only if it does not already exist.
docker compose up -d --build
```

Compose reads `.env` automatically. Rebuild with `--build` after engine changes.

Follow [local/README.md](local/README.md) for frontend startup, strategy
registration, Stop/Resume controls, restarts, and tests.

```sh
docker compose ps
docker compose logs -f provider-api engine
docker compose down
```

`down` keeps the local databases. Alpaca keys are passed only to the provider;
strategy containers have no credentials, host mounts, or network access.

The original `dashboard-api` remains available for database-backed history. The
old `snapshot` job is retained under the `legacy-snapshot` profile; its retired
provider read endpoints need migration before it can be used with this workflow.
