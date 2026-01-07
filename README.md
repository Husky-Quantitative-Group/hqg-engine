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