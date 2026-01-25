# dashboards.SKILL.md

## Name
dashboard_builder

## Description
Skill for designing and generating analytics dashboards (SQL queries, table models, and UI config) for this project.

## When to use
- User asks to build or modify dashboards.
- User wants metrics or charts over the Delta tables defined in this repo.

## Instructions
- Read `instructions/dashboards.md` and `README.md` before proposing anything.
- Always base metrics on the Delta tables defined under `main.<schema>.supplier_a_data` unless told otherwise.
- Generate:
  - SQL queries
  - Suggested visual types (bar/line/table)
  - Layout suggestions for BI tools (e.g., Databricks dash, Power BI)

## Commands
- `/design-dashboard`:
  - Ask the user for:
    - Business question
    - Time grain
    - Key metrics and dimensions
  - Propose:
    - 3–5 metrics
    - 3–5 visuals
    - The underlying SQL for each.
