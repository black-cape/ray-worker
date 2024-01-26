#!/usr/bin/make

include project.env

help:: ## This info
	@echo
	@cat Makefile | grep -E '^[a-zA-Z\/_-]+:.*?## .*$$' | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo

install: ## Install project dependencies
	@echo "Running $@"
	pre-commit install
	poetry install

build: ## build the docker image
	@echo "Running $@"
	docker compose --env-file=project.env build --force-rm --pull

info: ## show where all the services are
	@echo "Minio available at http://localhost:${MINIO_HTTP_PORT}"

up: ## Run the service and its docker dependencies, using a cached build if available
	@echo "Running $@"
	docker compose --env-file=project.env up --detach
	make info

lint: ## Run pylint over the main project files
	@echo "Running $@"
	poetry run pylint etl --rcfile .pylintrc

test: ## Run integration tests
	@echo "Running $@"
	poetry run coverage run --source etl -m pytest
	poetry run coverage report -m
	poetry run coverage html -d tests/htmlcov

stop: ## Stop this service's docker set
	@echo "Running $@"
	docker compose --profile ${PROJECT_PROFILE} --env-file=project.env --file docker-compose.yml down

generate-migration: ## Create a new database migration with alembic
	@echo "Running $@"
	sleep 5
	CURRENT_UID=$(shell id -u):$(shell id -g) docker compose --env-file=project.env --file docker-compose.yml exec ray_cast_iron_worker alembic revision --autogenerate && \
	CURRENT_UID=$(shell id -u):$(shell id -g) docker compose --env-file=project.env --file docker-compose.yml exec ray_cast_iron_worker alembic upgrade head

delete-db: ## Clear and reset this service's database (This will clear uncommitted migrations!)
	@echo "Running $@"
	docker exec -i ${POSTGRES_HOST} sh -c 'PGPASSWORD=${POSTGRES_PASSWORD} psql -U ${POSTGRES_USER} -d postgres -c "DROP DATABASE IF EXISTS \"${POSTGRES_DB}\";"'  || true
	docker exec -i ${POSTGRES_HOST} sh -c 'PGPASSWORD=${POSTGRES_PASSWORD} psql -U ${POSTGRES_USER} -d postgres -c "CREATE DATABASE \"${POSTGRES_DB}\";"'  || true
	find ./alembic/versions/ -type f \( -iname "*.py" ! -iname "checkpoint.*.py" \) | xargs rm -f

reset-service: ## WHILE SERVICE IS RUNNING: Clear the database, generate a migration, then repopulate the database
	@echo "Running $@"
	make delete-db
	make up
	sleep 5
	make generate-migration

clean: ## Tear down all project assets
	@echo "Running $@"
	docker compose --env-file=project.env --file docker-compose.yml down -v

setup: ## Perform application setup from a clean state
	@echo "Running $@"
	make install
	make build
	make up

version: ## Update this project version for a release [pass argument: type=(major,minor,patch)]
	@echo "Running $@"
	$(if $(type),@echo bumping version with type=$(type), $(error no bump type defined! add type=<major,minor,patch>))
	bash -c "./scripts/version.sh bump_version $(type) python ./pyproject.toml ./scripts/"
