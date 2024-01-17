FROM python:3.8-slim

WORKDIR /usr/src/app
ENV PYTHONPATH=/usr/src/app \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

WORKDIR ${WORKDIR}

# Copy using poetry.lock* in case it doesn't exist yet
COPY ./pyproject.toml ./poetry.lock* $WORKDIR/

# Install dependencies
RUN poetry install --no-root --no-dev

# Copy the application
COPY ./lib_ray_worker lib_ray_worker
COPY ./scripts scripts

CMD ["scripts/run_manager.sh"]
