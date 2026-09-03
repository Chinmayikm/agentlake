# The dbt / GE / OpenLineage toolbox.
#
# 3.12 and not 3.14, which is the whole reason this image exists rather than a
# second venv: dbt-core does not support 3.14 yet (dbt-labs/dbt-core#12098) and
# great-expectations 1.22.0 declares <3.14. Slim rather than alpine because
# these packages carry compiled wheels (pandas, numpy, pyarrow via GE) and
# musl builds either do not exist or are compiled from source at install time.
FROM python:3.12-slim

# git: `dbt deps` clones dbt_utils from GitHub.
# curl: the healthcheck-shaped waits in the Makefile targets.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /dbt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip freeze > /opt/pip-freeze.txt

# dbt would otherwise look in ~/.dbt. The project directory carries its own
# profiles.yml -- it holds no secret, because this Trino has no auth (ADR-005 #1
# makes the same argument for ClickHouse).
#
# TQDM_DISABLE: Great Expectations draws a tqdm progress bar per metric batch.
# Redirected to a file -- which is what `make quality > log` and CI both do --
# each refresh becomes its own line and 62 metrics produce several thousand
# characters of carriage-return noise around the actual result table.
ENV DBT_PROFILES_DIR=/dbt \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TQDM_DISABLE=1

# No ENTRYPOINT. Every caller is an explicit `docker compose run --rm dbt <cmd>`
# from the Makefile -- dbt, dbt-ol, or python for the Great Expectations
# checkpoint -- and an entrypoint that assumed one of them would just have to be
# overridden by the other two.
CMD ["dbt", "--version"]
