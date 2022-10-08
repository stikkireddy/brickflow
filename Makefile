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

coverage: check test


docker:
	docker build -t brickflow:latest .

poetry-install:
	@pip install poetry
	@poetry self add "poetry-dynamic-versioning[plugin]"