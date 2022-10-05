black-check:
	@black --check brickflow/engine
	@black --check brickflow/context

fmt:
	@black brickflow/engine
	@black brickflow/context

dev:
	@poetry install --all-extras
	@pip install cdktf
	@pip install "black>=22.8.0, <23.0.0"
	@pip install "prospector>=1.7.7, <2.0.0"

check: black-check
	@prospector --profile prospector.yaml