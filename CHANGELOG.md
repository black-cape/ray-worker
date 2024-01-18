# Ray Worker

### 5.0.0
- replace Clickhouse with Postgres as file status tracking database
- Removed FASTAPI which only existed to init Ray via FastAPI lifecycle method, and opt for a single main Thread


### 4.0.0
- Update to Pydantic 2

### 2.0.0

- No longer needing Mime Type based matching and reverting to initial Ray Worker Implementation
- Mime Type based routing should be moved to worker_run_method as configured in config
