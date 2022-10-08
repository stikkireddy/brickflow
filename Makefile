black-check:
	@poetry run black --check .

fmt:
	@poetry run black .

dev:
	@poetry install --all-extras

check: black-check
	@poetry run prospector --profile prospector.yaml

cov: check
	@poetry run coverage run --source=brickflow --omit "brickflow/sample_dags/*,sample_workflows/*,brickflow/tf/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage xml

coverage: check
	@poetry run coverage run --source=brickflow --omit "brickflow/sample_dags/*,sample_workflows/*,brickflow/tf/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage html

docker:
	docker build -t brickflow:latest .

poetry-install:
	@pip install poetry
	@poetry self add "poetry-dynamic-versioning[plugin]"