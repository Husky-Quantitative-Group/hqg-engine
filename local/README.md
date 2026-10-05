# Stage 1: mock execution

The stack uses one fake account, two broker contexts, the installed `mock-balanced`
strategy, authenticated HTTP sync, PostgreSQL runtime records and a mock dashboard
backend running the same control handlers as the AWS Lambda. The mock backend uses
SQLite to substitute for DynamoDB locally; the integration suite also verifies the
DynamoDB transaction adapter with Moto. No Alpaca credentials are needed or read.

From `hqg-engine`:

```sh
docker compose -p hqg-mvp -f compose.mock.yaml up -d --build
```

From `hqg-dashboard/frontend`:

```sh
CORE_API_URL=http://127.0.0.1:8011 npm run dev -- --host 0.0.0.0
```

The backend probes the actual frontend at `host.docker.internal:5173` and its own
account control route before granting permission. Configure `MVP_WEBSITE_URL` if
the frontend uses another local address. For a fully simulated website (fault
tests only), use `http://mock-readiness:8014/`.

Create a **fake local login**, then open <http://localhost:5173/engine>:

```sh
curl -X POST http://localhost:5173/api/test/login \
  -H 'Content-Type: application/json' -d '{"actor":"operator"}'
```

Curl does not install browser cookies. In the browser console on localhost:5173,
run this once and then reload:

```js
await fetch('/api/test/login', {
  method: 'POST', headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({actor: 'operator'})
});
location.href = '/engine';
```

`operator` can configure/resume both modes; `paper-operator` can resume only paper.
These login and fault routes exist only in the local mock server. The AWS routes
use the existing browser authorizer and account permissions.

1. Wait for connected, reconciled paused status.
2. Choose whether to include live, then configure the approved strategy.
3. Wait for the configuration to be applied; Start / Resume each enabled mode.
4. Stop a mode or Stop All. The action stays pending until a later sync reports
   that the provider gate applied the version. Cleanup remains separately visible.
5. Stop the frontend or mock backend to exercise the health interlock. Reopening
   it restores reporting; use a new Resume for each mode to restore execution.

The broker fixtures and balances are separate even when both modes run. Its
Alpaca-shaped request logs are available only inside the isolated mock network:

```sh
docker compose -p hqg-mvp -f compose.mock.yaml exec mock-dashboard \
  python -c 'import httpx; print(httpx.get("http://mock-broker:8012/test/paper/requests", trust_env=False).json())'
```

Provider or worker restart closes both gates. A new worker session needs explicit
Resume. The dashboard refuses a replacement provider boot until manually
re-authorized; it never automatically replaces a running owner. To replace it:

```sh
docker compose -p hqg-mvp -f compose.mock.yaml stop engine provider-api
# Wait at least 15 seconds after the old provider's last sync.
docker compose -p hqg-mvp -f compose.mock.yaml exec mock-dashboard python -m local.authorize_boot
docker compose -p hqg-mvp -f compose.mock.yaml up -d provider-api engine
```

Configuration and action/audit records remain in the dashboard volume. Permission
is never restored from storage. PostgreSQL stores account/mode portfolio mapping,
configuration, incidents, order IDs/outcomes, allocations and daily equity. The
legacy numeric portfolio 1 is labeled paper when mapped, preserving its history.
Legacy Stop/Resume/Liquidate and provider execution routes return 410.

Stop services without deleting either database:

```sh
docker compose -p hqg-mvp -f compose.mock.yaml down
```

## Acceptance checks

Use a dedicated disposable PostgreSQL database: the integration tests recreate
its tables. Both repositories must be present as siblings.

```sh
.venv/bin/pip install -r requirements-test.txt
MVP_TEST_DATABASE_URL=postgresql+asyncpg://mvp:FAKE-db-password@127.0.0.1:55432/mvp \
  .venv/bin/python -m pytest tests
```

The suite starts real loopback HTTP dashboard, provider, trading and quote servers.
It denies external DNS/socket destinations, disables proxies/redirects and rejects
missing or nonlocal mock URLs. The 25 integration scenarios, alongside 56 existing engine checks, cover authorization, immutable strategy
versions, transactional/idempotent control, mode isolation, pending Stop,
backend/control/website failures, delayed/malformed/stale sync, watchdog expiry
between child orders, cancellation failures/fills, explicit recovery, restart and
database failure, and reconciliation after a timeout following broker acceptance.
All fixtures use `FAKE-*` credentials. Real broker transport deliberately fails
startup in this milestone.

AWS definitions are in `hqg-dashboard/infra/accounts.tf`; they are not applied by
local startup. Provision the account state manually with its UUID, engine identity,
allowed users, approved strategy manifest/image digest and hashed sync secret.
The readiness URL must traverse the platform control route, which calls the same
account store/authentication dependency. Defaults deny permission until configured.
Real broker identity provisioning and TLS engine transport will be in to milestone 2.
