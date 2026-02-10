ARG PYTHON_VERSION
ARG UV_VERSION=0.9.21

RUN PYTHON_VERSION_ARG="${PYTHON_VERSION:-$(cat .python-version)}" \
    && export PYTHON_VERSION="$PYTHON_VERSION_ARG" \
    && echo "Using Python version: $PYTHON_VERSION"

FROM python:${PYTHON_VERSION}-slim as base

ENV PIP_DEFAULT_TIMEOUT=100 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_NO_CACHE_DIR=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=1 \
  PYTHONBUFFERED=1 \
  UV_GIT_LFS=1 \
  UV_NO_SYNC=1 \
  UV_PROJECT_ENVIRONMENT="/usr/local"


COPY --from=uv /uv /uvx /bin/

RUN apt-get update && \
  apt-get install -y --no-install-recommends git-lfs make openssh-client && \
  apt-get upgrade -y && \
  rm -f /var/lib/apt/lists/* && \
  git lfs install --system

FROM base as testing
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project  --no-editable --active --frozen
