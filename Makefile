black-check:
	@poetry run black --check .

fmt:
	@poetry run black .

dev:
	@poetry install --all-extras

check: black-check mypy
	@poetry run prospector --profile prospector.yaml

mypy:
	@poetry run mypy

cov: check
	@poetry run coverage run --source=brickflow --omit "brickflow/sample_dags/*,sample_workflows/*,brickflow/tf/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage xml

test:
	@poetry run coverage run --source=brickflow --omit "brickflow/sample_dags/*,sample_workflows/*,brickflow/tf/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage html

poetry:
	@poetry lock
	@poetry install --with dev

coverage: check test

docs:
	@poetry run mkdocs serve

deploy-docs:
	@poetry run mkdocs gh-deploy --force

docker:
	docker build -t brickflow:latest --build-arg CACHEBUST="$(shell date +%s)" .

poetry-install:
	@pip install poetry
	@poetry self add "poetry-dynamic-versioning[plugin]"


.PHONY: docs