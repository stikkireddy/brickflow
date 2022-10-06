black-check:
	@poetry run black --check brickflow/engine
	@poetry run black --check brickflow/context
	@poetry run black --check brickflow/hints
	@poetry run black --check tests

fmt:
	@poetry run black brickflow/engine
	@poetry run black brickflow/context
	@poetry run black brickflow/hints
	@poetry run black tests

dev:
	@poetry install --all-extras

check: black-check
	@poetry run prospector --profile prospector.yaml

cov:
	@poetry run coverage run --source=brickflow --omit "brickflow/sample_dags/*,brickflow/tf/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage xml

coverage:
	@poetry run coverage run --source=brickflow --omit "brickflow/sample_dags/*,brickflow/tf/*" -m pytest && \
	poetry run coverage report -m && \
	poetry run coverage html