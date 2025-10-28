# HQG Execution Engine

## File Structure

```
hqg-engine/
    src/
        ingestor/
            base.py              # Abstract ingestor interface for different data sources
            ibkr.py              # IBKR implementation
        strategy_client.py       # HTTP client to call strategy container APIs
        aggregator.py            # Aggregate multiple strategy allocations
        executor.py              # Convert allocations to orders and then execute
        routes.py                # FastAPI endpoints
        main.py                  # FastAPI app and orchestration logic
    
    config/
        strategies.yaml          # Strategy registration
        execution.yaml           # Execution settings
    
    docker/
        Dockerfile               # Execution engine container
        strategy-template/       # Template for containerizing individual strategies
            Dockerfile           # Strategy container template
            strategy_server.py   # FastAPI wrap for strategy class
            requirements.txt     # Strategy dependencies
    
    docker-compose.yml           # Orchestrate execution engine and individual strategy containers
    requirements.txt
    .gitignore
    README.md
```