black-check:
	@black --check brickflow/engine
	@black --check brickflow/context
	@black --check brickflow/hints
	@black --check tests

fmt:
	@black brickflow/engine
	@black brickflow/context
	@black brickflow/hints
	@black tests

dev:
	@poetry install --all-extras

check: black-check
	@prospector --profile prospector.yaml

cov:
	@poetry run coverage run -m pytest && poetry run coverage report -m && poetry run coverage xml

coverage:
	@poetry run coverage run -m pytest && poetry run coverage report -m && poetry run coverage html