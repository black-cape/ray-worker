# Ray Worker

### 5.0.0
- Simplified Ray bootstrap, previously this was tied to FastAPI life cycle, and the only reason FASTAPI was introduced as a dep
- Removed domain specific data model fields and revert Ray Worker to more generic form
- Removed Python dependencies no longer needed
- replace Clickhouse with Postgres as permanent storage to track File proessing status
- Wired up Alembic to support Postgres migration supported
- make repo publishable as a python package to ease integration

### 4.0.0
- Update to Pydantic 2

### 2.0.0

- No longer needing Mime Type based matching and reverting to initial Ray Worker Implementation
- Mime Type based routing should be moved to worker_run_method as configured in config
