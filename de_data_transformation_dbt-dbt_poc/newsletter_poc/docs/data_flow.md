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
  │  MARTS  (DBT_UDL)                          materialized: incremental│
  │                                             strategy: merge         │
  │                                                                     │
  │  wrk_newsletter              ← int_newsletter_joined + 2 seeds     │
  │  wrk_newsletter_interaction  ← stg_interaction + 5 seeds           │
  │  wrk_newsletter_category     ← stg_category (self-contained)       │
  │                                                                     │
  │  Deduplication · Reference enrichment · Null handling               │
  │  Audit columns · Schema contracts · Incremental merge               │
  └───────────────┬────────────────────────────────────────────────────┘
                  │
                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SNAPSHOTS  (DBT_UDL)                       SCD Type 2               │
  │                                                                     │
  │  snap_newsletter  ← wrk_newsletter                                 │
  │  Tracks full version history using check strategy on hash_value     │
  │  Maintains dbt_valid_from / dbt_valid_to for historical analysis    │
  └───────────────┬─────────────────────────────────────────────────────┘
                  │
                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  PUBLISH (UDL)             Hybrid Merge+Clone via stored procedure  │
  │                                                                     │
  │  UDL.NEWSLETTER              ← MERGE from DBT_UDL.WRK_NEWSLETTER   │
  │  UDL.NEWSLETTER_INTERACTION  ← MERGE from WRK_NEWSLETTER_INTERACTION│
  │  UDL.NEWSLETTER_CATEGORY     ← MERGE from WRK_NEWSLETTER_CATEGORY  │
  │  UDL.NEWSLETTER_SCD2         ← CLONE from SNAP_NEWSLETTER          │
  │  UDL.NEWSLETTER_HIST         ← INSERT (append from UDL.NEWSLETTER) │
  │                                                                     │
  │  MERGE: only changed rows are written — preserves Time Travel       │
  │  CLONE: instant metadata copy for SCD2 (history is built-in)        │
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
| `stg_newsletter_interaction_summary` | View | Aggregates `LISTAGG(DISTINCT delivery_system_type)` per newsletter — used to populate `actual_delivery_system_type` on the newsletter fact. |
| `stg_newsletter_interaction` | View | Parses interaction details from VARIANT. Classifies `device_type_code` from `user_agent` string (Desktop, Mobile, Tablet, Bot, Unknown). |
| `stg_newsletter_category` | View | Parses category fields (code, name, created timestamp) from VARIANT. Computes change-detection hash. |

**Design rationale:** Materialized as **tables** for predictable performance and consistent row counts during downstream processing. Each staging model has a single responsibility: parse one source table. Hash computation here enables efficient deduplication downstream. Full-load mode unions raw tables with archive tables for complete history.

---

### 3. Intermediate Layer

| Model | Materialization | Purpose |
|-------|-----------------|---------|
| `int_newsletter_joined` | Ephemeral (CTE) | Joins `stg_newsletter` with `stg_newsletter_recipient` and `stg_newsletter_interaction_summary`. Applies `ROW_NUMBER()` partitioned by `(tenant_code, code)` ordered by `kafka_timestamp DESC` to select only the latest version of each newsletter. |

**Design rationale:** Materialized as **ephemeral** — compiled inline as a CTE into the mart query. No physical table or view is created. This layer exists to separate join/dedup logic from business enrichment, keeping the mart model focused.

---

### 4. Marts Layer

| Model | Materialization | Unique Key | Key Logic |
|-------|-----------------|------------|-----------|
| `wrk_newsletter` | Incremental (merge) | `(tenant_code, code)` | Hash-based deduplication against existing records. Reference table lookups for `status_code` and `recipient_type_code`. Tenant-aware ID prefixing for `creator_id` / `modifier_id`. Null-safe `COALESCE` defaults. Post-hook updates `actual_delivery_system_type` from new interactions. Published as `UDL.NEWSLETTER`. |
| `wrk_newsletter_interaction` | Incremental (merge) | `(tenant_code, code)` | Latest-version ranking. Five reference table joins (interaction type, delivery system, recipient type, click type, block type). Device type classification. Tenant-aware recipient code prefixing. Published as `UDL.NEWSLETTER_INTERACTION`. |
| `wrk_newsletter_category` | Incremental (merge) | `(tenant_code, code)` | Latest-version ranking. Data source derivation. Null-safe defaults. Simplest of the three entities. Published as `UDL.NEWSLETTER_CATEGORY`. |

**Design rationale:** Materialized as **incremental with merge strategy** — only new/changed records (identified by hash comparison) are processed on each run. This mirrors the existing Scala pipeline's deduplication-then-merge pattern while being significantly simpler to maintain.

**Key features applied at this layer:**

| Feature | Implementation |
|---------|----------------|
| Schema Contracts | `contract: enforced: true` — every column's name and data type is validated at build time |
| Schema Change Protection | `on_schema_change: fail` — prevents silent column drift |
| Incremental Dedup | Hash-based: new records are compared against existing `hash_value` to skip unchanged data |
| Audit Columns | `dbt_loaded_at`, `dbt_run_id`, `dbt_batch_id`, `dbt_source_model`, `dbt_environment` |
| Row Count Tracking | `log_model_with_row_count()` post-hook captures rows affected per model |
| Composite Uniqueness | `dbt_utils.unique_combination_of_columns` test on `(tenant_code, code)` |

---

### 5. Snapshots Layer

| Model | Strategy | Tracked Columns | Purpose |
|-------|----------|-----------------|---------|
| `snap_newsletter` | Check | `hash_value`, `actual_delivery_system_type` | Maintains full SCD Type 2 history. When either tracked column changes, the current record is closed (`dbt_valid_to` set) and a new version is inserted. Supports `invalidate_hard_deletes` for tracking deletions. |

**Design rationale:** Only `wrk_newsletter` requires historical tracking (mirroring the existing `NEWSLETTER_HIST` table). The **check strategy** on the hash column efficiently captures any business-column change in a single comparison, while `actual_delivery_system_type` is tracked separately since it can change independently via post-hook updates. The snapshot is published as `UDL.NEWSLETTER_SCD2` via **clone** (Time Travel is redundant on SCD2 since the table itself is the history). The three fact tables are published via **MERGE** to preserve Snowflake Time Travel on the UDL targets.

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
| `UDL` | Staging tables, seeds, user-facing published tables |
| `DBT_UDL` | dbt work tables (wrk_*), snapshots |
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
    ├── Deduplicate against existing mart records (hash comparison)
    ├── Apply null defaults, tenant-aware ID prefixing
    ├── Add audit columns (run_id, batch_id, loaded_at)
    │
    ├── MERGE into mart tables (incremental)
    ├── Post-hook: update delivery system type from new interactions
    │
    └── Snapshot: track SCD-2 history for newsletters
```

| Metric | Value |
|--------|-------|
| Source tables | 3 |
| Staging models | 5 (views) |
| Intermediate models | 1 (ephemeral) |
| Mart models | 3 (incremental merge) |
| Snapshots | 1 (SCD-2 check) |
| Seeds | 6 (reference CSVs) |
| Custom macros | 7 |
| Automated tests | 30+ (schema, uniqueness, accepted values, contracts) |
| Dashboard queries | 30+ across 9 categories |
