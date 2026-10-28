# HQG Execution Engine

This serves as a containerized trading execution system, including a trading engine, worker to capture past metrics, and a FastAPI server for an external dashboard.

Docker is the recommended way to run the full application in a production environment. This runs all services: PostgreSQL database, FastAPI API server, trading engine, and snapshot job scheduler.

### Quick Start (VM Deployment)

1. **SSH into your virtual machine**
   ```bash
   ssh user@your-vm-ip
   ```

2. **Clone the repository**
   ```bash
   git clone https://github.com/Husky-Quantitative-Group/hqg-engine.git
   cd hqg-engine
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   nano .env
   # Edit .env with your actual credentials
   ```

4. **Start all services**
   ```bash
   docker-compose up --build -d
   ```

5. **Verify services are running**
   ```bash
   docker-compose ps
   # After, all services should show "Up" status
   ```

6. **Test the API (from within VM)**
   ```bash
   curl http://localhost:8000/health
   # This should return: {"status":"healthy"}
   ```

   It may also be necessary to configure the firewall to allow inbound traffic, as well as the security group in any external dashboard's cloud provider.

### Common Docker Operations

**View logs:**
```bash
docker-compose logs -f

docker-compose logs -f api
docker-compose logs -f engine
docker-compose logs -f snapshot
docker-compose logs -f db
```

**Restart a service:**
```bash
docker-compose restart api
docker-compose restart engine
docker-compose restart snapshot
```

**Stop all services:**
```bash
docker-compose down
```

**Rebuild after code changes:**
```bash
git pull
docker-compose up --build -d
```

### Externally Accessing the API
- **API**: http://VM_IP:8000
- **API Docs**: http://VM_IP:8000/docs
- **Health Check**: http://VM_IP:8000/health

## Configuration Setup

### Environment Variables (.env)

- **Database Configuration**
  - `DATABASE_URL`: PostgreSQL connection string (format: `postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DATABASE`)
  - `POSTGRES_DB`: Database name
  - `POSTGRES_USER`: Database user
  - `POSTGRES_PASSWORD`: Database password

- **Execution Provider Selection**
  - `PROVIDER`: Set to `"alpaca"` or `"ib" accordingly

- **Alpaca Configuration**
  - `ALPACA_API_KEY`: Your Alpaca API key
  - `ALPACA_SECRET_KEY`: Your Alpaca secret key
  - `ALPACA_PAPER`: Set to `"true"` for paper trading, `"false"` for live trading

- **IBKR Configuration**
  - `IBKR_HOST`: IBKR TWS/Gateway host
  - `IBKR_PORT`: IBKR TWS/Gateway port
  - `IBKR_CLIENT_ID`: Unique client ID for this connection

### Config Files

- **`src/config/portfolio.yaml`**: Portfolio and strategy configuration
  - Define which strategies are active
  - Set portfolio weights for each strategy

## Alternative data for strategies

The engine continues using Alpaca/IBKR snapshots for securities OHLCV. A strategy
can additionally declare provider-prefixed alternative series in its Python class:

```python
class MyStrategy(Strategy):
    universe = ["SPY"]
    alt_data = ["FRED.GDP", "FRED.DGS10"]
    # cadence and on_data follow the existing strategy interface.
```

Read history with `data.alt_series("FRED.GDP")`, or the most recent observation
with `data.alt_value("FRED.GDP")`. History keys are observation dates in naive UTC;
they are not publication dates. IDs are normalized to uppercase. Strategies without `alt_data` use the
existing securities-only input and do not create a datafeed client.

Set these in the engine's `.env` (placeholders are in `.env.example`):

```dotenv
ALT_DATA_HISTORY_START=2025-01-01
FRED_API_KEY=your_key_here
```

The history start is required only when alternative series are declared. It is an
inclusive observation-date bound, not an indicator warmup period. FRED requires
an API key; 

Alternative history loads once at startup and refreshes immediately before due
strategy calls. Shared series are fetched once per decision batch, grouped by
provider, over the full configured range through today in UTC. Refetching this
range captures revisions to old observations. There is **no periodic background
polling**, and repeated rebalances against previous targets do not fetch alternative
data. Synchronous network calls run in worker threads.

The latest received revision for each observation date is exposed through an
immutable history snapshot. Provider availability timestamps and actual receipt
times are kept separately in memory. Live decisions include newly received data.
Provider timestamps can be estimates rather than measured publication times.

If a fetch fails, the engine retains cached history for inspection, defers affected
strategy calls, and retries on subsequent engine cycles no sooner than 60 seconds
after failure. Providers fetch concurrently with an overall deadline of
`FETCH_TIMEOUT_SECONDS` in `src/alt_data.py` (120 seconds per provider).
The decision batch waits until each provider finishes or reaches its deadline;
successful results remain usable when another provider fails or times out.
Series are fetched individually within each provider worker so one unusable series
does not discard valid sibling histories. Shared series are still deduplicated.

A timeout does not stop a running thread. The engine retains one worker per provider
until it exits, shares it across concurrent callers, and starts no replacement while
it is still running—even after the 60-second retry cooldown expires. Late results
from timed-out workers are discarded rather than overwriting the cache.
Completed bars are queued in memory and retried oldest first, at most one pending
call per strategy per cycle. Deferred historical bars use alternative data known
at retry time. Pending calls and strategy indicator state do not survive restart.
Only alternative-data fetch failures retain a bar for retry. Once inputs are
available, a strategy exception, `None`, unexpected output, or `Hold()` retains
its last successful target allocation without retrying the strategy call.
Explicit empty targets and `Liquidate()` still request an allocation to cash.

When alternative data is unavailable, a strategy retains its previous allocation.
If it has never produced an allocation, it remains unallocated and logs that fact.