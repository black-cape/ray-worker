# Ray Worker

### 5.0.0
- Simplified Ray bootstrap, previously this was tied to FastAPI life cycle, and the only reason FASTAPI was introduced as a dep
- Removed domain specific data model fields and revert Ray Worker as more generic
- Removed Python Deps no longer needed
- replace Clickhouse with Postgres as file status tracking database
- Alembic + Postgres migration supported
- make repo publishable to a python package

### 4.0.0
- Update to Pydantic 2

### 2.0.0

- No longer needing Mime Type based matching and reverting to initial Ray Worker Implementation
- Mime Type based routing should be moved to worker_run_method as configured in config
