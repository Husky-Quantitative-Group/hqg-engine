# HQG Execution Engine

The execution engine uses real Alpaca integration, `compose.yaml` and `.env`.
The stack includes PostgreSQL, the provider, isolated
strategy workers. Runtime AWS substitutes, fake login and fake readiness have
been removed. Registration and pinned package delivery use the dashboard AWS
backend over HTTPS. An AWS account and deployed dashboard are required for
interactive development; see [dashboard setup](../hqg-dashboard/docs/local_setup.md).

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
Docker and Python development install dependencies from `requirements.txt`.
No separate algorithms checkout is required.

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

The provider connects to Alpaca directly, ignoring the proxy variables. If the host
reaches Alpaca only through a proxy, set `ALPACA_PROXY_URL` to it, e.g.
`http://proxy.example.edu:3128`.

Strategies receive completed historical bars at their declared daily, weekly,
monthly or quarterly cadence (daily by default). The provider uses Alpaca's
market calendar, including holidays and early closes, and waits one minute after
the final session close for bar publication. Repeated or older bar timestamps
do not advance a strategy. If any universe symbol is missing, the worker waits
for the complete period instead of mixing dates. New or resumed strategy
contexts start with the latest eligible completed bar; they do not replay missed
periods or restore indicator history.

`CLOSE_TO_NEXT_OPEN` waits for the following session's open before evaluating the
completed bar. `CLOSE_TO_CLOSE` evaluates after the completed bar is available;
live market orders cannot guarantee the historical closing price. Orders sent
outside market hours follow Alpaca's normal order handling.

The original `dashboard-api` remains available for database-backed history. The
old `snapshot` job is retained under the `legacy-snapshot` profile; its retired
provider read endpoints need migration before it can be used with this workflow.

`DASHBOARD_SYNC_URL` must be HTTPS and include the API Gateway stage. Dashboard
credentials go only to the provider; the worker uses `PROVIDER_WORKER_SECRET`.
Registration/configuration do not start trading: wait for acknowledgements and
explicitly Resume after readiness and broker reconciliation. New provider boots
require the administrative replacement procedure in the dashboard setup guide.

Use the [offline test guide](docs/offline-testing.md) for synthetic credentials,
Moto, disposable PostgreSQL and Docker isolation checks. No existing volumes are
deleted or migrated. `docker compose down -v` deletes PostgreSQL history. Manually
export wanted legacy strategy source and re-upload it through the real dashboard.
