# Local development with Alpaca

This stack connects to the real Alpaca Trading and Market Data APIs. Paper trading
is the default; `ALPACA_PAPER` selects which account mode to use. The local
dashboard still substitutes SQLite for DynamoDB and provides local login and
readiness checks. No AWS deployment is needed.


Note: This local dir can be removed once real AWS services replace this mock local substitution (and remove COPY local/ ./local/ from the Dockerfile).

## Start

From `hqg-engine`:

```sh
# For a new setup only (keep an existing .env):
cp .env.example .env
# Edit .env: set ALPACA_API_KEY, ALPACA_SECRET_KEY, and ALPACA_PAPER=true.
# Also set POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, and DATABASE_URL.
# DATABASE_URL must address the Compose database at host db, port 5432.
docker network create hqg_network # Only if it does not already exist.
docker compose up -d --build
```

The original `db` service, `postgres_data` volume, database environment variables,
health checks, restart policies, and build/runtime proxy settings are retained.
`DATABASE_URL` must match the PostgreSQL username, password, and database, for
example `postgresql+asyncpg://hqg:development-password@db:5432/hqg`. Changing
PostgreSQL environment variables does not reset a database already in the volume.
The engine worker uses an internal control network; the provider also connects to
the existing `hqg_network` for database and Alpaca access.

Use **Trading API paper keys** from your Alpaca dashboard. The provider sends them
to `https://paper-api.alpaca.markets`; data comes from `https://data.alpaca.markets`.
`ALPACA_DATA_FEED=iex` works with a paper-only account. Use `sip` only if your
account has that subscription. The provider uses the authenticated account. See Alpaca's
[authentication](https://docs.alpaca.markets/us/docs/authentication) and
[paper trading](https://docs.alpaca.markets/us/v1.4.2/docs/paper-trading) documentation.

Compose automatically reads `.env`. If it already exists, add the settings from
`.env.example` without overwriting your other values. The credentials are
`ALPACA_API_KEY` and `ALPACA_SECRET_KEY`. Set `ALPACA_PAPER=true` for Paper or
`ALPACA_PAPER=false` for Live, using the corresponding account's keys. An unset or
blank `ALPACA_PAPER` defaults to Paper; other values are rejected. Only the selected
mode is configured for a provider run.

The existing Alpaca SDK clients handle trading and market data. Keys go only to
the provider container; workers read account/market data through its authenticated
read API. User strategy containers receive no secrets, mounts, Docker socket or
network. Compose and the sandbox use the `hqg-engine-local` image tag; rebuild
with `--build` when changing engine code.

The original `dashboard-api` serves database-backed history. The old `snapshot`
definition is retained under the `legacy-snapshot` profile and is excluded from
normal startup because it calls retired provider read endpoints. Migrating that
job is separate from replacing the Alpaca mock.

From `hqg-dashboard/frontend`:

```sh
IS_PROD=0 CORE_API_URL=http://127.0.0.1:8011 \
  npm run dev -- --host 0.0.0.0 --port 5173 --strictPort
```

Keep the frontend running. The backend checks it at
`host.docker.internal:5173` and checks its own control route before granting
execution permission. Set `LOCAL_WEBSITE_URL` for a different frontend address.
For a backend-only readiness fixture, set
`LOCAL_WEBSITE_URL=http://mock-readiness:8014/`.

Sign in as the local operator (or POST `{"actor":"operator"}` to
`http://127.0.0.1:8011/test/login` from your browser), then:

1. Create/copy a strategy in the dashboard and save its Python files.
2. On the Engine page, register the saved version and wait for its acknowledgement.
3. Set its paper allocation and apply the configuration. Leave Live disabled.
4. Resume paper trading. Stop closes the gate and reconciles engine-owned orders;
   it retains positions.

There is no built-in execution strategy. The editable balanced template is a
normal saved package and requires registration like any other user strategy.
Universes support 1–200 unique uppercase stock tickers, including ETFs and class
symbols such as `SPY`, `IEF`, and `BRK.B`. The account and feed must support those
symbols. Packages remain immutable and execute in isolated Docker containers.

## Strategy data and execution

The worker keeps its existing five-second cycle. Each enabled strategy receives
Alpaca's latest minute OHLCV bars and the current portfolio. Prices for sizing
orders come from real quotes. Declared daily/weekly/monthly/quarterly cadence is
not scheduled in this patch; the same bar may be supplied on successive cycles.

The engine starts the isolated strategy container, calls `on_data()`, and receives
target weights, Hold, or Liquidate. Containers stay running between calls to
preserve strategy state. Resource limits, timeouts, package validation, and
Stop/Resume controls remain in effect.

Alpaca fills asynchronously; pending orders prevent another rebalance. The
provider rechecks execution permission immediately before transmitting an order,
and does not automatically resubmit orders with uncertain outcomes. Missing or
invalid data and strategy errors pause execution. Orders submitted outside market
hours follow Alpaca's order handling; market-aware scheduling is deferred.

For optional real-money execution, replace `ALPACA_API_KEY` and
`ALPACA_SECRET_KEY` with your live account's keys and set `ALPACA_PAPER=false`,
then follow the provider replacement steps below. Disable Paper, configure Live,
and resume Live explicitly in the dashboard. Enabling the unconfigured mode fails
closed.

## Restart or rebuild

A new provider boot must be authorized after the old owner stops; a worker restart
also requires Resume. To replace a provider after changing engine code:

```sh
docker compose stop engine provider-api
# Wait at least 15 seconds after the old provider's last sync.
docker compose build
docker compose up -d mock-dashboard
docker compose exec mock-dashboard python -m local.authorize_boot
docker compose up -d provider-api engine
```

Configuration, packages and portfolio/order history remain in the local volumes;
both modes need Resume. This command also clears any previously stored local
image-ID requirement.

The former `compose.mock.yaml` stack is replaced by `compose.yaml`, which keeps
the original Compose project naming and `postgres_data` volume. Fake broker orders, account IDs, and
built-in strategy approvals are not suitable for reuse with a real account. Stop
the old stack before starting this one to free port 8011:

```sh
docker stop hqg-mvp-engine-1 hqg-mvp-provider-api-1 hqg-mvp-mock-dashboard-1 hqg-mvp-mock-broker-1 hqg-mvp-mock-readiness-1 hqg-mvp-mock-db-1
```

This retains the old volumes and saved artifacts. Copy any old strategy files you
need into the new dashboard and register them again.

Stop the new stack without deleting databases:

```sh
docker compose down
```

## Tests

The fake Alpaca server now exists only in `tests/integration/broker_fixture.py`.
Tests deny external network access and never read real Alpaca credentials. Use a
**dedicated disposable PostgreSQL database**: integration tests recreate its tables.
Both repositories must be present as siblings.

```sh
.venv/bin/pip install -r requirements-test.txt
docker build -t hqg-engine-alpaca-test .
docker run -d --name hqg-alpaca-test-db -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=hqg_test -p 127.0.0.1:55433:5432 postgres:18
MVP_TEST_DATABASE_URL=postgresql+asyncpg://test:test@127.0.0.1:55433/hqg_test \
STRATEGY_SANDBOX_IMAGE=hqg-engine-alpaca-test .venv/bin/python -m pytest tests
# After testing:
docker rm -f hqg-alpaca-test-db
```

The local AWS substitutes and their login/fault routes remain for development.
AWS strategy artifact packaging/registration is still not deployed; this change
makes the local dashboard-controlled workflow use Alpaca.
