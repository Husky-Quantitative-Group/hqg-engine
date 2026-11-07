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

Strategies receive completed historical bars at their declared daily, weekly,
monthly or quarterly cadence (daily by default). The provider uses Alpaca's
market calendar, including holidays and early closes, and waits one minute after
the final session close for bar publication. Repeated or older bar timestamps
do not advance a strategy. If any universe symbol is missing, the worker waits
for the complete period instead of mixing dates. New or resumed strategy
contexts start with the latest eligible completed bar. A strategy that declares
`lookback = N` first has the previous N completed bars replayed through
`on_data` (signals discarded), so its first live decision uses that history.

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

## Alternative data

Dashboard-delivered strategies can declare alternative series alongside their
stock universe:

```python
class Macro(Strategy):
    universe = ["SPY"]
    alt_data = ["FRED.GDP"]

    def on_data(self, data, portfolio):
        gdp = data.alt_value("FRED.GDP")
        history = data.alt_series("FRED.GDP")
        # Return TargetWeights, Hold, or Liquidate as usual.
```

The algorithms release must support `alt_data` metadata and `Slice.alt_history`.
Set `FRED_API_KEY` in `.env`. Compose forwards it only to `provider-api`.
Alternative history starts inclusively `ceil(N × 1.10)` weekdays before the current
New York date, where N is the strategy's `lookback` (default **1** when absent).
Today and weekends are excluded. The 10% buffer approximates holidays without a
broker calendar request; it does not guarantee N trading sessions. The default
N=1 therefore subtracts 2 weekdays. This rolling window is recalculated on each
request. Shared series use the earliest start date
required by configured strategies in that mode (the longest lookback). Lookback
defines the minimum history requested; returned history is passed through without
trimming for individual strategies, so series may start on different dates.
N counts weekdays before the buffer even when securities bars are weekly or monthly.
Choose a sufficient lookback for sparse series and replay: observations before the
window are not carried forward or fetched automatically.
`KALSHI.<EXACT_MARKET_TICKER>` uses public daily candles without
credentials; contract discovery/rolling are not provided by this integration.

The network-enabled provider owns `hqg-datafeed`, its cache, and external
credentials. The worker requests data through the authenticated
`/internal/alternative-data` endpoint, which checks the current control context
before and after fetching and allows only series declared by configured strategies
in that mode. Neither the internal-only worker nor network-free strategy
containers make external datafeed requests. Sandboxes receive serialized history
and construct immutable alternative mappings in `Slice`.

History is loaded on the first active decision and refreshed when a new or deferred
strategy bar needs processing, not on every five-second worker poll. Shared series
are deduplicated within a decision batch. The full calculated observation range
through the current UTC date is refetched so older revisions can be discovered.
Providers fetch concurrently with a 120-second deadline (`FETCH_TIMEOUT_SECONDS`
in `src/alt_data.py`) and a minimum 60-second failure backoff. Timed-out threads
remain tracked until they exit; no overlapping replacement is launched and late
results are discarded. Successful series remain usable when another series fails.

Live calls receive the newest available revision per observation date, ordered
oldest first. Lookback replay instead selects revisions with `available_at`
strictly before each historical bar's timezone-aware **start** timestamp. This is
a conservative replay cutoff, not an exchange-close/publication-time estimate.
It avoids exposing today's later revisions to historical calls, but cannot
reconstruct vintages the datafeed does not supply. Provider availability timestamps
may themselves be estimates. Replayed signals remain discarded as on main.

When alternative data is unavailable, only affected strategies are deferred and
their previous targets are retained. An invalid alternative-data lookback also
defers only that strategy and is logged; it does not prevent fetching shared series
for strategies with valid lookbacks. Fixing it requires a configuration update.
For fetch failures, completed bars remain queued in memory,
oldest first, at most one per strategy per worker cycle. Configuration changes and
mode stops discard those queues; they do not survive restart. Once inputs are
available, execution errors retain main's behavior of reporting a strategy failure
and closing the provider gate. No general exception-to-Hold policy is introduced.
Main's initial-target rule also remains: a mode does not rebalance until every
strategy has supplied its first target, and repeated Hold decisions do not trade.
