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

clean: ## Tear down all project assets
	@echo "Running $@"
	docker compose --env-file=project.env --file docker-compose.yml down -v

setup: ## Perform application setup from a clean state
	@echo "Running $@"
	make install
	make build
	make up
