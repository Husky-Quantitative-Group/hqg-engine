# Offline testing

No AWS account or broker credentials are needed. Use one test virtual environment
for both repositories.

From the root of `hqg-dashboard`:

```sh
pip install -r requirements-test.txt
bash infra/scripts/build_layers.sh
python -m pytest tests
```

Then, from the root of `hqg-engine`:

```sh
pip install -r requirements-test.txt
python -m pytest tests
```

Tests use synthetic AWS/Alpaca credentials, disable personal AWS credential lookup,
and block external connections. They do not load personal `.env` files. Local
HTTP, PostgreSQL and Docker connections are allowed where needed.

## PostgreSQL and Docker integration tests

Integration tests skip when their prerequisites are missing. From the root of
`hqg-engine`, start a disposable database and build the strategy image:

```sh
docker run -d --rm --name hqg-offline-postgres \
  -e POSTGRES_USER=hqg_test -e POSTGRES_PASSWORD=FAKE-test-password \
  -e POSTGRES_DB=hqg_test -p 127.0.0.1:55439:5432 postgres:18
docker build -t hqg-engine-strategy-test .
export MVP_TEST_DATABASE_URL=postgresql+asyncpg://hqg_test:FAKE-test-password@127.0.0.1:55439/hqg_test
export STRATEGY_SANDBOX_IMAGE=hqg-engine-strategy-test
python -m pytest tests/integration
docker stop hqg-offline-postgres
```

**Use only a disposable database: these tests drop its application tables.**
The commands above do not use or delete existing Compose volumes. If Docker
cannot write its build configuration, set `BUILDX_CONFIG=/tmp/hqg-buildx`.

## Limitations

Offline tests do not verify deployed IAM, API Gateway routing/CORS/authorizers,
CAS login/callbacks, real TLS connections, AWS service guarantees or latency,
Lambda's managed runtime, or actual Alpaca trading. These require separate
account-backed checks.
