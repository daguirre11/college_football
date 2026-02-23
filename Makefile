UV := uv
export UV_GIT_LFS := 1

BLUE=\033[0;34m
NC=\033[0m # No Color
.SHELLFLAGS=-xc

FOLDERS=college_football

.PHONY: all \
	check \
	check-ci \
	check-format \
	check-lint \
	check-lock \
	check-pre-commit \
	check-types \
	format \
	help \
	lint \
	test \
	test-only

all: check test

check-pre-commit: ## Run the pre-commit checks that do not require docker on all files
	${UV} run pre-commit run --all-files

check-lock: ## Check uv.lock and pyproject.toml are coherent
	${UV} lock --locked

format: ## Run format
	@${UV} run ruff format .

check-format: ## Run format check
	@${UV} run ruff format --check .

check-types: ## Run type checker
	@${UV} run mypy ${FOLDERS}

lint: ## Run linter
	@${UV} run ruff check --fix .

check-lint: ## Run lint checker
	@${UV} run ruff check .

check: check-pre-commit check-lock check-format check-types check-lint ## Run all checks

check-ci: check-lock check-format check-types check-lint ## Run all checks for ci/cd

install: ## Just update the environment
	@echo "\n${BLUE}Initialize git lfs...${NC}\n"
	git lfs install
	@echo "\n${BLUE}Running UV update...${NC}\n"
	@${UV} run python --version
	@${UV} lock --no-upgrade
	@${UV} sync --locked
	@echo "\n${BLUE}Show outdated packages...${NC}\n"
	@${UV} tree --outdated --locked
	@echo "\n${BLUE}pre-commit install${NC}\n"
	@${UV} run pre-commit install
	@${UV} run pip-audit --desc

test-only: ## Run a single test like so make test_name='tests/rt_correction/test_data_preparation.py' test-only
	@${UV} run python -m pytest --durations=0 -vv $(test_name)

test: ## Run all the tests with code coverage. You can also `make test tests/test_my_specific.py`
	@echo "\n${BLUE}Running pytest with coverage...${NC}\n"
	@${UV} run coverage erase;
	@${UV} run coverage run --branch -m pytest \
		--junitxml=junit/test-results.xml -vv
	@${UV} run coverage report --fail-under=98
	@${UV} run coverage html
	@${UV} run coverage xml

help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; \
		{printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}
