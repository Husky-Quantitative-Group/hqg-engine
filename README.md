# HQG Execution Engine

The execution engine uses real Alpaca integration, `compose.yaml` and `.env`.
The stack includes PostgreSQL, the provider, isolated
strategy workers. Runtime AWS substitutes, fake login and fake readiness have
been removed. Real strategy registration/package delivery and provider HTTPS
transport remain unfinished; this cleanup does not complete AWS integration.

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

Set the account/engine UUIDs, dashboard service URL and distinct dashboard/worker
secrets listed in `.env.example`; Compose requires them and passes dashboard
credentials only to the provider. Use the dashboard's real CAS/JWT login.

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
