STORAGES := "dev.storages.yaml"
PROJECT_NAME := "kafka"
ENV := "--env-file .env"

py *args:
    uv run {{args}}

install:
    uv venv --clear && uv run pre-commit install && uv sync --all-extras --all-groups

storages:
    docker compose -f {{STORAGES}} {{ENV}} -p {{ PROJECT_NAME }} up -d --build --remove-orphans

storages-down:
	docker compose -f {{STORAGES}} {{ENV}} -p {{ PROJECT_NAME }} down

cli *args:
	just py python3 ./app {{args}}

producers *args:
	just cli producers {{args}}

consumers *args:
	just cli consumers {{args}}
