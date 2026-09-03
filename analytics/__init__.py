"""Analytics layer (ADR-006).

Two different Pythons live under this directory, which is worth stating once:

- ``Dockerfile`` + ``requirements.txt`` build the **3.12** toolbox image that
  runs dbt, Great Expectations and dbt-ol. Nothing in that image imports this
  package; the build context copies only ``requirements.txt``.
- ``trino_client.py`` is a **3.14** module, imported from the repo venv by
  ``scripts/analytics_verify.py`` and ``scripts/seed_iceberg.py``.

The split exists because dbt and GE do not support Python 3.14 and this repo
does. See ADR-006 #2.
"""
