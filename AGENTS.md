# Repository Guidelines

## Project Structure & Module Organization

This repository is a documentation-first Snowflake learning project. Main lesson content lives in `sql/` as paired files such as `07_views.md` and `07_views.sql`; update both when changing a chapter. Supplemental examples live in `dbt/` for transformation models, `airflow/` for the sample DAG, and `datasets/` for source data such as `datasets/events_sample.json`. Root-level docs like `README.md` and `sql/README.md` index the chapter flow and should stay in sync with any added or renamed lessons. Keep the learning flow consistent: `RAW` -> `STAGING` -> `MART`, and remember that `RAW.RAW_EVENTS` is the Chapter 02 practice table while `RAW.RAW_EVENTS_PIPE` is the pipeline source used from Chapter 03 onward.

## Build, Test, and Development Commands

There is no single repo-wide build step. Use the command that matches the area you edited:

- `dbt debug --project-dir dbt` validates the local dbt profile and Snowflake connection.
- `dbt run --project-dir dbt` executes the sample models in `dbt/models/`.
- `dbt test --project-dir dbt` runs schema tests from `dbt/models/schema.yml`.
- `python -m py_compile airflow/snowflake_event_pipeline.py` checks the Airflow DAG for syntax errors.
- `snow sql -q "SELECT COUNT(*) FROM RAW.RAW_EVENTS_PIPE"` is useful for quick Snowflake verification from the terminal when SnowCLI is available.

SQL chapter files are intended to be run manually in Snowsight or a Snowflake worksheet.

## Coding Style & Naming Conventions

Follow the existing style before introducing new patterns. Keep chapter files numbered with zero-padded prefixes like `03_snowpipe.sql`. Lesson SQL should preserve the current teaching structure with `What you learn`, `Run this first`, `Check`, and `Try this` sections. Use lowercase SQL keywords in dbt models, short two-space indentation in SQL, and descriptive uppercase object names for Snowflake objects such as `MART.V_SALES_DETAIL`. In Python, follow PEP 8 and preserve the current straightforward Airflow DAG style. In YAML, use two-space indentation and keep model names aligned with their SQL filenames. Prefer Japanese Snowflake documentation links (`docs.snowflake.com/ja/`) when adding or updating references.

## Testing Guidelines

Prefer lightweight validation tied to the edited area. For dbt changes, run `dbt run` and `dbt test`; add column tests in `dbt/models/schema.yml` when new fields are introduced. For Airflow changes, keep the DAG importable and verify task SQL still targets the `LEARN_DB` example objects. For lesson SQL and Markdown, confirm prerequisites, object names, and chapter references remain consistent, especially around chapter dependencies such as Chapter 03 before `dbt/` and Chapters 03-04 before `airflow/`.

## Commit & Pull Request Guidelines

Recent history uses concise conventional-style subjects such as `fix: 03章...` and `fix: 05章...`. Keep commits focused, start with a type like `fix:` or `docs:`, and mention the affected chapter or area. Pull requests should summarize learner-facing impact, list touched paths, link the related issue when applicable, and include screenshots only when documentation visuals or rendered output changed.
