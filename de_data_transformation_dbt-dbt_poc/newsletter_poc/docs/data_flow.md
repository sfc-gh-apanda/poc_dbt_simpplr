# Newsletter dbt Pipeline — Data Flow & Architecture

## Overview

This project replaces a Scala/Snowpark ETL pipeline with a **dbt-native** implementation on Snowflake. It processes newsletter, interaction, and category events ingested from Kafka, transforming raw semi-structured (VARIANT) data into clean, analytics-ready fact tables.

The pipeline is organized into **five distinct layers**, each with a clear purpose and materialization strategy.

---

## Architecture Diagram

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        KAFKA INGESTION                             │
  │   Newsletter events  ·  Interaction events  ·  Category events     │
  └───────────────┬──────────────────┬──────────────────┬──────────────┘
                  │                  │                  │
                  ▼                  ▼                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SOURCES  (SHARED_SERVICES_STAGING)                                │
  │                                                                     │
  │  VW_ENL_NEWSLETTER    VW_ENL_NEWSLETTER    VW_ENL_NEWSLETTER       │
  │                       _INTERACTION          _CATEGORY              │
  │                                                                     │
  │  Raw VARIANT rows — header (tenant info) + domain_payload (JSON)   │
  └───────────────┬──────────────────┬──────────────────┬──────────────┘
                  │                  │                  │
                  ▼                  ▼                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  STAGING  (UDL)                                    materialized: table │
  │                                                                     │
  │  Narrow delta: WHERE created_datetime >= start AND <= end           │
  │  (start/end from Airflow via batch_run_id watermark)               │
  │                                                                     │
  │  stg_newsletter             Parse newsletter JSON, extract          │
  │  stg_newsletter_recipient   LATERAL FLATTEN recipients, LISTAGG     │
  │  stg_newsletter_interaction Parse interactions, classify device     │
  │  stg_newsletter_interaction Aggregate delivery systems per NL       │
  │    _summary                                                         │
  │  stg_newsletter_category    Parse category JSON                     │
  │                                                                     │
  │  + Hash computation (MD5) for change detection                      │
  └───────────────┬──────────────────┬──────────────────┬──────────────┘
                  │                  │                  │
                  ▼                  │                  │
  ┌────────────────────────┐        │                  │
  │  INTERMEDIATE          │        │                  │
  │  (ephemeral — CTE)     │        │                  │
  │                        │        │                  │
  │  int_newsletter_joined │        │                  │
  │  Joins newsletter +    │        │                  │
  │  recipients + inter-   │        │                  │
  │  action summary, then  │        │                  │
  │  ranks to latest per   │        │                  │
  │  (tenant, code)        │        │                  │
  └───────────┬────────────┘        │                  │
              │                     │                  │
              ▼                     ▼                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SEEDS  (UDL)                              6 Reference CSV Tables   │
  │  ref_newsletter_status  ·  ref_newsletter_recipient_type           │
  │  ref_newsletter_interaction_type  ·  ref_newsletter_delivery_...   │
  │  ref_newsletter_click_type  ·  ref_newsletter_block_type           │
  │                                                                     │
  │  Used by marts for code lookups (e.g. status → status_code)        │
  ├─────────────────────────────────────────────────────────────────────┤
  │                                                                     │
  │  MARTS  (DBT_UDL)                          materialized: table      │
  │                                             (delta-only per run)   │
  │                                                                     │
  │  wrk_newsletter              ← int_newsletter_joined + 2 seeds     │
  │  wrk_newsletter_interaction  ← stg_interaction + 5 seeds           │
  │  wrk_newsletter_category     ← stg_category (self-contained)       │
  │                                                                     │
  │  Hash-dedup against UDL published tables · Reference enrichment    │
  │  Audit columns · Schema contracts · Contains only current delta    │
  └───────────────┬────────────────────────────────────────────────────┘
                  │
                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  PUBLISH (UDL)            HIST-as-master via stored procedure       │
  │                                                                     │
  │  UDL.NEWSLETTER_HIST         ← UPDATE deactivate + INSERT new      │
  │    (master SCD-2 accumulator, active_flag tracked)                 │
  │  UDL.NEWSLETTER              ← TRUNCATE + INSERT WHERE active=TRUE │
  │    (derived current state, Time Travel preserved)                   │
  │  UDL.NEWSLETTER_INTERACTION  ← MERGE delta from WRK (Time Travel) │
  │  UDL.NEWSLETTER_CATEGORY     ← MERGE delta from WRK (Time Travel) │
  │                                                                     │
  │  All within a single transaction for atomicity                      │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## Layer Details

### 1. Sources

| Schema | Tables | Description |
|--------|--------|-------------|
| `SHARED_SERVICES_STAGING` | `VW_ENL_NEWSLETTER`, `VW_ENL_NEWSLETTER_INTERACTION`, `VW_ENL_NEWSLETTER_CATEGORY` | Reconstructed Kafka messages. Each row contains a `HEADER` (tenant info as VARIANT) and `DOMAIN_PAYLOAD` (full event JSON as VARIANT). |

**Design rationale:** These mirror the existing production staging views. Data arrives here via Kafka ingestion — the dbt pipeline reads from this point forward.

**Tests applied:** `not_null`, `unique` on IDs; `accepted_values` on event types.

---

### 2. Staging Layer

| Model | Materialization | Key Transformations |
|-------|-----------------|---------------------|
| `stg_newsletter` | View | Parses 25+ fields from VARIANT JSON. Extracts channel flags (email, SMS, Teams, Slack, intranet). Derives `is_deleted` from event type. Computes `MD5` hash over all business columns for downstream change detection. |
| `stg_newsletter_recipient` | View | `LATERAL FLATTEN` on the recipients array. `LISTAGG` to aggregate recipient names into a single string per newsletter. |
| `stg_newsletter_interaction_summary` | Ephemeral | Reads from `stg_newsletter_interaction` (no second source scan). Aggregates `LISTAGG(DISTINCT delivery_system_type)` per newsletter — used to populate `actual_delivery_system_type` on the newsletter fact. |
| `stg_newsletter_interaction` | Table | Parses interaction details from VARIANT. Classifies `device_type_code` from `user_agent` string (Desktop, Mobile, Tablet, Bot, Unknown). |
| `stg_newsletter_category` | View | Parses category fields (code, name, created timestamp) from VARIANT. Computes change-detection hash. |

**Design rationale:** Materialized as **tables** for predictable performance and consistent row counts during downstream processing. Each staging model has a single responsibility: parse one source table. Hash computation here enables efficient deduplication downstream. Full-load mode unions raw tables with archive tables for complete history.

**Narrow delta:** Staging reads only the time window between `data_process_start_time` and `data_process_end_time` — matching the Scala pipeline's `dataProcessStartTime`/`dataProcessEndTime` pattern. When called from Airflow, these are passed via `--vars` from the BATCH_RUN table. Defaults (`2000-01-01` / `9999-12-31`) allow standalone dbt builds without Airflow.

---

### 3. Intermediate Layer

| Model | Materialization | Purpose |
|-------|-----------------|---------|
| `int_newsletter_joined` | Ephemeral (CTE) | Joins `stg_newsletter` with `stg_newsletter_recipient` and `stg_newsletter_interaction_summary`. Applies `ROW_NUMBER()` partitioned by `(tenant_code, code)` ordered by `kafka_timestamp DESC` to select only the latest version of each newsletter. |

**Design rationale:** Materialized as **ephemeral** — compiled inline as a CTE into the mart query. No physical table or view is created. This layer exists to separate join/dedup logic from business enrichment, keeping the mart model focused.

---

### 4. Marts Layer

| Model | Materialization | Key Logic |
|-------|-----------------|-----------|
| `wrk_newsletter` | Table (delta-only) | Hash-based deduplication against `UDL.NEWSLETTER`. Reference table lookups for `status_code` and `recipient_type_code`. Tenant-aware ID prefixing. Contains only current run's new/changed records. Published to `NEWSLETTER_HIST` then derived to `UDL.NEWSLETTER`. |
| `wrk_newsletter_interaction` | Table (delta-only) | Hash-based deduplication against `UDL.NEWSLETTER_INTERACTION`. Five reference table joins. Device type classification. Published via MERGE to `UDL.NEWSLETTER_INTERACTION`. |
| `wrk_newsletter_category` | Table (delta-only) | Hash-based deduplication against `UDL.NEWSLETTER_CATEGORY`. Data source derivation. Published via MERGE to `UDL.NEWSLETTER_CATEGORY`. |

**Design rationale:** Materialized as **table** (rebuilt each run with delta only). Each wrk table contains only the current run's new/changed records, deduped against the published UDL tables. This mirrors the existing Scala pipeline's pattern where work tables are truncated and reloaded each run.

**Key features applied at this layer:**

| Feature | Implementation |
|---------|----------------|
| Schema Contracts | `contract: enforced: true` — every column's name and data type is validated at build time |
| Delta-Only Dedup | Hash-based: new records are compared against published UDL `hash_value` to skip unchanged data |
| Audit Columns | `batch_run_id` (Airflow), `dbt_loaded_at`, `dbt_run_id`, `dbt_batch_id`, `dbt_source_model`, `dbt_environment` |
| Row Count Tracking | `log_model_with_row_count()` post-hook captures rows affected per model |
| Composite Uniqueness | `dbt_utils.unique_combination_of_columns` test on `(tenant_code, code)` |

---

## Reference Data (Seeds)

Six CSV-managed lookup tables provide standardized code mappings:

| Seed | Purpose | Example |
|------|---------|---------|
| `ref_newsletter_status` | Status labels → codes | `DRAFT` → `NLS001` |
| `ref_newsletter_recipient_type` | Recipient type mapping | `PEOPLE` + followers → `NLRT001` |
| `ref_newsletter_interaction_type` | Interaction classification | `OPEN` → `NLIT002` |
| `ref_newsletter_delivery_system_type` | Channel codes | `email` → `NLDST001` |
| `ref_newsletter_click_type` | Click classification | `LINK_CLICK` → `NLCT001` |
| `ref_newsletter_block_type` | Content block codes | `TEXT` → `NLBT001` |

---

## Cross-Cutting Concerns

### Observability & Logging

```
on-run-start  →  log_run_start()   →  DBT_RUN_LOG (insert)
                                          │
  each model  →  log_model_execution() →  DBT_MODEL_LOG (insert)
                                          │
on-run-end    →  log_run_end()     →  DBT_RUN_LOG (update with duration, status, counts)
```

All logging writes to `DBT_EXECUTION_RUN_STATS` schema. Four summary views are provided:
- `V_DAILY_RUN_SUMMARY` — runs per day with pass/fail rates
- `V_MODEL_EXECUTION_HISTORY` — per-model timing trends
- `V_RECENT_FAILURES` — last 50 failures for triage
- `V_INCREMENTAL_MODEL_STATS` — row counts and duration for incremental models

### Monitoring & Dashboards

**28 monitoring views** and **30+ dashboard queries** covering:
- Run metrics & duration trends
- Error analysis & consecutive failure detection
- Cost attribution by model (via Snowflake Account Usage)
- Test pass rates & coverage by layer
- Data freshness & row count trends
- Data reconciliation (source → staging → mart completeness)
- Snapshot consistency checks

### Schema Isolation

Schemas are aligned with the existing architecture for clear separation:

| Schema | Purpose |
|--------|---------|
| `SHARED_SERVICES_STAGING` | Raw Kafka data (input) |
| `UDL` | Staging tables, seeds, published tables (NEWSLETTER derived from NEWSLETTER_HIST) |
| `DBT_UDL` | dbt delta work tables (wrk_*) |
| `UDL_BATCH_PROCESS` | Stored procedures (publish, archive, retry) |
| `DBT_EXECUTION_RUN_STATS` | Run & model logs, test results, build artifacts |

---

## Data Flow Summary

```
Kafka Messages (VARIANT)
    │
    ├── Parse JSON fields, extract tenant from header
    ├── Flatten arrays (recipients), aggregate (delivery systems)
    ├── Compute MD5 hash for change detection
    │
    ├── Join newsletter + recipients + interaction summary
    ├── Rank by kafka_timestamp → keep latest per (tenant, code)
    │
    ├── Lookup reference codes (status, recipient type, etc.)
    ├── Deduplicate against published UDL tables (hash comparison)
    ├── Apply null defaults, tenant-aware ID prefixing
    ├── Add audit columns (batch_run_id, dbt_run_id, dbt_batch_id, loaded_at)
    │
    ├── Build delta-only work tables (current run's changes)
    │
    └── Publish: NEWSLETTER_HIST (SCD-2) → UDL.NEWSLETTER (active)
                 INTERACTION/CATEGORY → MERGE into UDL
                 Stamps published_by_run_id on all target tables
```

| Metric | Value |
|--------|-------|
| Source tables | 3 |
| Staging models | 5 (views) |
| Intermediate models | 1 (ephemeral) |
| Mart models | 3 (delta-only tables) + 1 sentinel |
| Seeds | 6 (reference CSVs) |
| Custom macros | 7 |
| Automated tests | 30+ (schema, uniqueness, accepted values, contracts) |
| Dashboard queries | 30+ across 9 categories |

---

## Airflow Integration & End-to-End Traceability

When called from Airflow, the dbt pipeline receives three key variables from the BATCH_RUN table:

```
Airflow DAG run
  └→ task_initiate_process
       └→ prc_udl_batch_handler("initiate") → batch_run_id, start_time, end_time
  └→ EXECUTE DBT PROJECT ... --vars '{"batch_run_id": 101, "data_process_start_time": "...", "data_process_end_time": "..."}'
  └→ task_complete_process
       └→ prc_udl_batch_handler("complete", batch_run_id)
```

### Traceability columns on every UDL record

| Column | Source | Purpose |
|--------|--------|---------|
| `batch_run_id` | Airflow `BATCH_RUN` table | Ties to DAG run, source data window — consistent across retries |
| `dbt_run_id` | dbt `invocation_id` | Ties to specific dbt invocation — useful for dbt-internal debugging |
| `published_by_run_id` | dbt `invocation_id` at publish time | Identifies the publish event — consistent across all entities |

### Why `batch_run_id` is immune to retry splits

Even if a smart retry creates a new dbt `invocation_id`, the `batch_run_id` remains the same because it originates from Airflow's `task_initiate_process`, which only runs once per DAG execution. This provides the same end-to-end traceability as the Scala pipeline's `CREATED_BATCH_RUN_ID`.
