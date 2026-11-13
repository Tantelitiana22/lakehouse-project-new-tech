# YAML-driven Dagster schedules

## Intent

Today `build_definitions()` returns only assets and resources, so Dagster has no
jobs, schedules or sensors: the Automation tab is empty and nothing runs on a
clock. The YAML contract already drives *what* to ingest and *how* to write it.
This change extends the same contract to *when* to run, so the cron is edited in
a file rather than in Python.

## Goals

- A dedicated, optional `use_case/<case>/schedule.yaml` declared by the
  manifest (`schedule_file`, default `schedule.yaml`).
- Each entry produces one asset job and one schedule.
- `cron` is a standard five-field expression; `timezone` is an IANA name.
- `enabled: true` registers the schedule as RUNNING; `false` registers it as
  STOPPED (visible in the UI, easy to turn on by hand).
- Selection is explicit: omit it to target every asset, or narrow it with
  `groups` / `assets`.
- Invalid cron, timezone, name or selection fails at code-location load with a
  readable error, never silently creating an empty schedule.
- Editing the YAML takes effect without rebuilding the Docker image.

## Non-goals

- Per-asset schedules, sensors, and auto-materialisation on data arrival.
- Partitioned/backfill schedules.
- Editing schedules from the Dagster UI (the YAML stays the source of truth).
- Schedules for use cases other than the one the process loads.

## Anti-goals

- A schedule that silently targets nothing, or a typo that pauses automation
  without telling anyone.
- Requiring a `docker compose build` to change one line of cron.

## Contract

```yaml
schedules:
  - name: daily_lakehouse_refresh
    description: Landing -> Bronze -> Gold -> semantic check
    cron: "0 6 * * *"          # 06:00 every day
    timezone: Europe/Paris
    enabled: true
    # Omit `selection` to run the whole graph.
    selection:
      groups: [landing, bronze, dbt, semantic]
```

`selection` accepts exactly one of:

- `groups: [<group_name>, ...]` — matches the existing asset groups
  (`landing`, the Bronze group from each source, `dbt`, `semantic`).
- `assets: [["lakehouse", "bronze", "dvf_mutations_77"], ...]` — explicit asset
  key paths.

## How it works

1. `UseCaseConfig` gains `schedule_file` (optional, default `schedule.yaml`,
   resolved next to the manifest).
2. New `orchestration/schedules.py`:
   - Pydantic models `ScheduleSpec` and `SchedulesFile`.
   - cron validated with Dagster's own `is_valid_cron_string` (private helper,
     noted in a comment) and timezone with `zoneinfo.ZoneInfo`.
   - `load_schedule_specs(path)` returns `[]` when the file is absent.
   - `build_schedules(specs)` returns `ScheduleDefinition`s, each wrapping a
     `define_asset_job`.
3. `build_definitions()` merges those schedules into its `Definitions(...)`.
4. `docker-compose.yml` bind-mounts `./use_case:/app/use_case:ro` into both
   Dagster services, so the file in the repo is the file the daemon reads.
5. Tests cover parsing, invalid cron/timezone, the three selection forms,
   enabled/stopped mapping, the missing file, and that `build_definitions`
   exposes the schedule.
6. README gets a short "Schedules" section with the reload procedure.

## Operational note

Both Dagster processes load the workspace in-process (`-w /app/workspace.yaml`),
so after editing the YAML run `docker compose restart dagster-webserver
dagster-daemon` (or restart the stack). No image rebuild is needed once the bind
mount exists. The daemon service is already in `docker-compose.yml`, so ticks
will fire.

## Approaches considered

1. **Dedicated `schedule.yaml` per use case (chosen).** Keeps triggers separate
   from asset definitions, supports several schedules later, matches the
   manifest style.
2. **A `schedule:` block in `use_case.yaml`.** Fewer files, but mixes two
   concerns and makes the manifest grow with scheduling detail.
3. **Cron keys on each asset YAML.** Fine-grained but fragments the trigger and
   produces one job per asset.

## Assumptions

- One schedule for the whole graph is enough for the first version; the list
  shape leaves room for per-layer entries later.
- `enabled: true` is used in the shipped example so automation works out of the
  box; the model default is `false` when the key is omitted.
