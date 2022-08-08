FROM python:3.8-slim

WORKDIR /app/
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

# Copy using poetry.lock* in case it doesn't exist yet
COPY ./pyproject.toml ./poetry.lock* /app/

# Install dependencies
RUN poetry install --no-root --no-dev

# Copy the application
COPY ./etl /app/etl

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8002", "etl.app_manager:app"]