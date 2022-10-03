black-check:
	@black --check brickflow/engine
	@black brickflow/context
	@black --check brickflow/adapters

lint:
	@black brickflow/engine
	@black brickflow/context
	@black brickflow/adapters

dev:
	@poetry install --all-extras
	@pip install cdktf
	@pip install "black>=22.8.0, <23.0.0"
	@pip install "prospector>=1.7.7, <2.0.0"

check: black-check
	@prospector --profile prospector.yaml