# Newsletter dbt Pipeline — Project Validation Document

**Project:** Simpplr Newsletter dbt PoC
**Database:** COMMON_TENANT_DEV (Snowflake)
**Execution:** Snowflake Native dbt (EXECUTE DBT PROJECT)
**Version:** 1.0.0

---

## 1. Executive Summary

This project replaces the existing Scala/Snowpark newsletter ETL pipeline with a **dbt-native** implementation on Snowflake. It processes newsletter, interaction, and category events ingested from Kafka, transforming raw semi-structured (VARIANT) data into clean, analytics-ready tables.

### Key Outcomes

| Metric | Value |
|--------|-------|
| Source entities | 3 (Newsletter, Interaction, Category) |
| dbt models | 10 (5 staging + 1 intermediate + 3 marts + 1 sentinel) |
| Snapshots (SCD-2) | 1 (snap_newsletter) |
| Reference seeds | 6 (CSV-managed lookup tables) |
| Custom macros | 14 |
| Automated tests | 51 (schema, uniqueness, accepted values, contracts) |
| Stored procedures | 8 (merge helper, publish, archive, retry, artifact logging) |
| Monitoring views | 28 |
| Dashboard queries | 30+ across 9 categories |

---

## 2. Architecture Overview

### 2.1 Data Flow Diagram

```
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                           KAFKA INGESTION                                  │
  │      Newsletter events  ·  Interaction events  ·  Category events          │
  └──────────┬───────────────────────┬───────────────────────┬─────────────────┘
             │                       │                       │
             ▼                       ▼                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  SOURCES  (SHARED_SERVICES_STAGING)                                        │
  │  VW_ENL_NEWSLETTER · VW_ENL_NEWSLETTER_INTERACTION · VW_ENL_NEWSLETTER_    │
  │  _CATEGORY  +  ENL_*_ARCHIVE tables (used during full-load)                │
  │  Raw VARIANT rows — HEADER (tenant info) + DOMAIN_PAYLOAD (event JSON)     │
  └──────────┬───────────────────────┬───────────────────────┬─────────────────┘
             │                       │                       │
             ▼                       ▼                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STAGING  (UDL schema)                                 materialized: table  │
  │  stg_newsletter              Parse 25+ fields from VARIANT JSON             │
  │  stg_newsletter_recipient    LATERAL FLATTEN recipients, LISTAGG            │
  │  stg_newsletter_interaction  Parse interactions, classify device type        │
  │  stg_newsletter_interaction  Aggregate delivery systems per newsletter      │
  │    _summary                                                                 │
  │  stg_newsletter_category     Parse category JSON                            │
  │  + MD5 hash computation for change detection                                │
  │  + Full-load mode: UNION ALL raw + archive tables                           │
  └──────────┬───────────────────────┬───────────────────────┬─────────────────┘
             │                       │                       │
             ▼                       │                       │
  ┌──────────────────────────┐       │                       │
  │  INTERMEDIATE (ephemeral)│       │                       │
  │  int_newsletter_joined   │       │                       │
  │  Join NL + recipients +  │       │                       │
  │  interaction summary     │       │                       │
  │  ROW_NUMBER → latest per │       │                       │
  │  (tenant_code, code)     │       │                       │
  └──────────┬───────────────┘       │                       │
             │                       │                       │
             ▼                       ▼                       ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  SEEDS (UDL schema)                          6 Reference CSV Tables         │
  │  ref_newsletter_status · ref_newsletter_recipient_type                      │
  │  ref_newsletter_interaction_type · ref_newsletter_delivery_system_type      │
  │  ref_newsletter_click_type · ref_newsletter_block_type                      │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │  MARTS (DBT_UDL schema)                      materialized: incremental     │
  │                                               strategy: merge              │
  │  wrk_newsletter              ← int_newsletter_joined + 2 seeds             │
  │  wrk_newsletter_interaction  ← stg_interaction + 5 seeds                   │
  │  wrk_newsletter_category     ← stg_category (self-contained)               │
  │  pipeline_complete           ← sentinel view (triggers publish + archive)   │
  │                                                                             │
  │  Hash-based dedup · Reference enrichment · Schema contracts                 │
  │  Audit columns · Incremental merge on (tenant_code, code)                   │
  └──────────┬──────────────────────────────────────────────────────────────────┘
             │
             ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  SNAPSHOTS (DBT_UDL schema)                    SCD Type 2                   │
  │  snap_newsletter  ← wrk_newsletter                                         │
  │  Check strategy on hash_value + actual_delivery_system_type                 │
  │  Maintains dbt_valid_from / dbt_valid_to for full history                   │
  └──────────┬──────────────────────────────────────────────────────────────────┘
             │
             ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  PUBLISH (UDL schema)           Hybrid Merge+Clone via stored procedure     │
  │  UDL.NEWSLETTER              ← MERGE from DBT_UDL.WRK_NEWSLETTER           │
  │  UDL.NEWSLETTER_INTERACTION  ← MERGE from DBT_UDL.WRK_NEWSLETTER_INTERACTION│
  │  UDL.NEWSLETTER_CATEGORY     ← MERGE from DBT_UDL.WRK_NEWSLETTER_CATEGORY  │
  │  UDL.NEWSLETTER_SCD2         ← CLONE DBT_UDL.SNAP_NEWSLETTER               │
  │  UDL.NEWSLETTER_HIST         ← INSERT from UDL.NEWSLETTER (append)         │
  │                                                                             │
  │  MERGE: only changed rows written — preserves Snowflake Time Travel         │
  │  CLONE: instant for SCD2 (table IS the history — Time Travel redundant)     │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │  ARCHIVE (SHARED_SERVICES_STAGING)          Post-publish raw data archival  │
  │  VW_ENL_NEWSLETTER → ENL_NEWSLETTER_ARCHIVE (INSERT + DELETE)              │
  │  VW_ENL_NEWSLETTER_INTERACTION → ENL_NEWSLETTER_INTERACTION_ARCHIVE        │
  │  VW_ENL_NEWSLETTER_CATEGORY → ENL_NEWSLETTER_CATEGORY_ARCHIVE              │
  └─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Schema Layout

| Schema | Purpose | Objects |
|--------|---------|---------|
| `SHARED_SERVICES_STAGING` | Raw Kafka data (input) | 3 source tables + 3 archive tables |
| `UDL` | Staging tables, seeds, user-facing published tables | 5 staging tables, 6 seed tables, 3 MERGE-published fact tables + NEWSLETTER_SCD2 (cloned) + NEWSLETTER_HIST |
| `DBT_UDL` | dbt work tables and snapshots | 3 wrk_* tables, 1 snapshot, 1 sentinel view |
| `UDL_BATCH_PROCESS` | Stored procedures | 8 procedures (merge helper, publish, archive, retry, artifact logging) |
| `DBT_EXECUTION_RUN_STATS` | Audit and observability | Run logs, model logs, test results, build results, monitoring views |

---

## 3. Object Mapping: Scala Pipeline → dbt Pipeline

### 3.1 Table Mapping

| Scala/Current Architecture | dbt Architecture | Relationship |
|---------------------------|------------------|--------------|
| `SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER` | `{{ source('shared_services_staging', 'VW_ENL_NEWSLETTER') }}` | Same table, read via `source()` |
| `SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION` | `{{ source('shared_services_staging', 'VW_ENL_NEWSLETTER_INTERACTION') }}` | Same table, read via `source()` |
| `SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY` | `{{ source('shared_services_staging', 'VW_ENL_NEWSLETTER_CATEGORY') }}` | Same table, read via `source()` |
| `UDL_BATCH_PROCESS.SHARED_STG_*` | `UDL.STG_NEWSLETTER`, `STG_NEWSLETTER_INTERACTION`, etc. | Staging layer (parsing + hashing) |
| `UDL_BATCH_PROCESS.WRK_*` | `DBT_UDL.WRK_NEWSLETTER`, `WRK_NEWSLETTER_INTERACTION`, `WRK_NEWSLETTER_CATEGORY` | Work/mart tables (dedup + enrich) |
| `UDL.NEWSLETTER` | `UDL.NEWSLETTER` (via MERGE from `DBT_UDL.WRK_NEWSLETTER`) | User-facing published table (Time Travel preserved) |
| `UDL.NEWSLETTER_INTERACTION` | `UDL.NEWSLETTER_INTERACTION` (via MERGE) | User-facing published table (Time Travel preserved) |
| `UDL.NEWSLETTER_CATEGORY` | `UDL.NEWSLETTER_CATEGORY` (via MERGE) | User-facing published table (Time Travel preserved) |
| `UDL.NEWSLETTER_HIST` | `UDL.NEWSLETTER_HIST` (appended during publish) | Historical snapshots of newsletter state |
| N/A (new) | `UDL.NEWSLETTER_SCD2` (via clone of `SNAP_NEWSLETTER`) | Full SCD-2 history with dbt_valid_from/to |

### 3.2 Process Mapping

| Scala Process | dbt Equivalent | How |
|--------------|----------------|-----|
| Batch run tracking (`BATCH_RUN`, `PROCESS_RUN`) | `DBT_RUN_LOG`, `DBT_MODEL_LOG` | `on-run-start`/`on-run-end` hooks, model post-hooks |
| Hash-based deduplication | `LEFT JOIN ... ON hash_value` in incremental mode | Same MD5 hash logic over business columns |
| ROW_NUMBER latest version | `ROW_NUMBER() OVER (PARTITION BY tenant_code, code ORDER BY kafka_timestamp DESC)` | `int_newsletter_joined` ephemeral model |
| Reference table lookups | `LEFT JOIN {{ ref('ref_newsletter_status') }}` | 6 dbt seed tables |
| Publish to UDL (DELETE+INSERT) | Hybrid Merge+Clone | MERGE for fact tables (preserves Time Travel, touches only changed rows), CLONE for SCD2 |
| Archive raw data | `PRC_DBT_ARCHIVE_RAW_DATA` procedure | INSERT into archive + DELETE from raw |
| SCD-2 history | `snap_newsletter` (dbt snapshot, check strategy) | Tracks `hash_value` + `actual_delivery_system_type` |

---

## 4. Data Loading Patterns

### 4.1 Incremental Merge

**What:** Only new/changed records are processed on each run. The wrk_* tables use `incremental` materialization with `merge` strategy.

**How it works:**
1. Staging rebuilds the full parsed dataset from source tables
2. Incremental filter: `WHERE created_datetime >= MAX(dbt_loaded_at)` from the target table
3. Hash-based dedup: new records are compared against existing `hash_value` — unchanged records are skipped
4. dbt MERGE: matched records (same `tenant_code` + `code`) are updated; unmatched are inserted

**Unique keys:** `(tenant_code, code)` for all three entities.

### 4.2 SCD Type 2 (Slowly Changing Dimension)

**What:** Full version history tracking for newsletters using dbt snapshots.

**How it works:**
- `snap_newsletter` uses the **check strategy** on `hash_value` and `actual_delivery_system_type`
- When either column changes, the current record is closed (`dbt_valid_to` = current timestamp) and a new version is inserted
- `invalidate_hard_deletes = True` — if a record disappears from the source, the snapshot marks it as deleted
- Published as `UDL.NEWSLETTER_SCD2` for downstream consumption

### 4.3 Hash-Based Change Detection

**What:** MD5 hash computed over all business columns at the staging layer. Used downstream for:
- Deduplication (skip unchanged records during incremental merge)
- SCD-2 change detection (snapshot triggers on hash change)

**Columns hashed (newsletter):** name, subject, sender_address, channels, recipients, status, category, template, theme, creator_id, modifier_id, timestamps, is_deleted, is_archived, reply_to_address, and more.

### 4.4 Atomic Publish (Hybrid Merge+Clone)

**What:** All three user-facing UDL tables + SCD-2 + NEWSLETTER_HIST are updated atomically in a single transaction.

**How it works:**
1. `pipeline_complete` sentinel model depends on all 3 wrk_* models + snap_newsletter
2. Its post-hook calls `PRC_DBT_PUBLISH_TO_TARGET` stored procedure
3. **MERGE** for fact tables — dynamic MERGE built from `INFORMATION_SCHEMA` column metadata:
   - `WHEN MATCHED AND src.HASH_VALUE != tgt.HASH_VALUE THEN UPDATE` — only changed rows are written
   - `WHEN NOT MATCHED THEN INSERT` — new rows are added
   - `WHEN NOT MATCHED BY SOURCE THEN DELETE` — removed rows are purged
4. **CLONE** for `NEWSLETTER_SCD2` — instant metadata copy (SCD2 is inherently historical)
5. **INSERT** to `NEWSLETTER_HIST` — appends current state with `published_by_run_id` and `published_at`
6. All within `BEGIN TRANSACTION ... COMMIT`

**Why hybrid Merge+Clone:**
- **MERGE preserves Snowflake Time Travel** on UDL fact tables — consumers can use `AT(TIMESTAMP => ...)` for point-in-time queries, disaster recovery, and audit
- **MERGE is performant at scale** — at 1B+ rows, only changed rows are written (hash comparison), avoiding full-table rewrites
- **CLONE for SCD2 is optimal** — the table itself tracks complete history via `dbt_valid_from`/`dbt_valid_to`, making Time Travel redundant on this specific table
- **Dynamic column resolution** via `PRC_MERGE_PUBLISH` helper — adapts automatically to schema changes without hardcoded column lists

### 4.5 Raw Data Archival

**What:** After publish, processed raw records are moved from source tables to archive tables.

**How it works:**
1. Post-hook on `pipeline_complete` calls `PRC_DBT_ARCHIVE_RAW_DATA`
2. Procedure inserts processed records into `ENL_*_ARCHIVE` tables
3. Deletes the same records from `VW_ENL_*` source tables
4. Uses `data_process_end_time` as the boundary
5. All within a single transaction

**Full-load mode:** When `is_full_load: true`, staging models `UNION ALL` raw + archive tables to reconstruct the complete dataset.

### 4.6 Delete Handling

**What:** The `NEWSLETTER_DELETED` event type sets `is_deleted = TRUE` on the newsletter record.

**How it works:**
- `stg_newsletter` derives `is_deleted` from `TYPE = 'NEWSLETTER_DELETED'`
- The hash includes `is_deleted`, so the wrk_* table is updated via MERGE
- `snap_newsletter` captures the delete as a new SCD-2 version

---

## 5. Project Structure

```
newsletter_poc/
├── dbt_project.yml              Project configuration, variables, hooks
├── packages.yml                 dbt_utils + dbt_expectations
├── profiles.yml                 Snowflake connection (database: COMMON_TENANT_DEV)
│
├── models/
│   ├── _sources.yml             Source definitions + 19 source-level tests
│   ├── staging/
│   │   ├── _stg.yml             Staging model documentation
│   │   ├── stg_newsletter.sql
│   │   ├── stg_newsletter_recipient.sql
│   │   ├── stg_newsletter_interaction.sql
│   │   ├── stg_newsletter_interaction_summary.sql
│   │   └── stg_newsletter_category.sql
│   ├── intermediate/
│   │   └── int_newsletter_joined.sql
│   └── marts/
│       ├── _marts.yml           Contracts + 26 model-level tests
│       ├── wrk_newsletter.sql
│       ├── wrk_newsletter_interaction.sql
│       ├── wrk_newsletter_category.sql
│       └── pipeline_complete.sql
│
├── snapshots/
│   └── snap_newsletter.sql      SCD-2 check strategy
│
├── seeds/                       6 reference CSV lookup tables
│
├── macros/
│   ├── audit/                   audit_columns(), row_hash(), hash_key()
│   ├── logging/                 log_run_start/end(), log_model_execution(),
│   │                            log_model_with_row_count(), log_failed_models()
│   ├── publish/                 publish_to_target()
│   ├── archive/                 archive_raw_data()
│   ├── data_source_code.sql     Tenant-aware data source derivation
│   └── generate_schema_name.sql Custom schema naming (use custom_schema as-is)
│
├── setup/                       Snowflake infrastructure scripts
│   ├── account_bootstrap.sql    Database, schemas, source tables, sample data
│   ├── audit_setup.sql          DBT_RUN_LOG, DBT_MODEL_LOG, summary views
│   ├── publish_archive_setup.sql Publish + archive stored procedures
│   ├── retry_setup.sql          Smart retry (model manifest, retry procedures)
│   ├── test_logging_setup.sql   Artifact-based test/build logging
│   ├── monitoring_queries.sql   28 monitoring views
│   ├── dashboard_queries.sql    30+ dashboard tile queries
│   └── dataload_pattern_tests.sql  6-round validation script
│
└── docs/
    ├── data_flow.md             Architecture and data flow documentation
    └── project_validation_document.md  This document
```

---

## 6. Model Details

### 6.1 Staging Models (5 models, materialized as `table`)

| Model | Source | Key Transformations |
|-------|--------|---------------------|
| `stg_newsletter` | `VW_ENL_NEWSLETTER` | Parses 25+ fields from VARIANT JSON. Extracts channel flags (email, SMS, Teams, Slack, intranet). Derives `is_deleted` from event type. Extracts `tenant_code` from nested `HEADER:tenant_info` JSON. Computes MD5 hash over all business columns. |
| `stg_newsletter_recipient` | `VW_ENL_NEWSLETTER` | `LATERAL FLATTEN` on the recipients array. `LISTAGG` to aggregate recipient names into a single comma-separated string per newsletter. |
| `stg_newsletter_interaction_summary` | `VW_ENL_NEWSLETTER_INTERACTION` | Aggregates `LISTAGG(DISTINCT delivery_system_type)` per newsletter+tenant — populates `actual_delivery_system_type` on the newsletter work table. |
| `stg_newsletter_interaction` | `VW_ENL_NEWSLETTER_INTERACTION` | Parses interaction details from VARIANT. Classifies `device_type_code` from `user_agent` string (Desktop, Mobile, Tablet, Bot, Unknown). |
| `stg_newsletter_category` | `VW_ENL_NEWSLETTER_CATEGORY` | Parses category fields (code, name, created timestamp) from VARIANT. Computes change-detection hash. |

**Full-load support:** All staging models conditionally `UNION ALL` with archive tables when `is_full_load: true` or the entity-specific full-load flag is set.

### 6.2 Intermediate Model (1 model, materialized as `ephemeral`)

| Model | Purpose |
|-------|---------|
| `int_newsletter_joined` | Joins `stg_newsletter` + `stg_newsletter_recipient` + `stg_newsletter_interaction_summary`. Applies `ROW_NUMBER()` partitioned by `(tenant_code, code)` ordered by `kafka_timestamp DESC` to keep only the latest version of each newsletter. |

### 6.3 Mart Models (3 work tables + 1 sentinel, materialized as `incremental`)

| Model | Unique Key | Enrichment | Published As |
|-------|-----------|------------|--------------|
| `wrk_newsletter` | `(tenant_code, code)` | 2 seed lookups (status, recipient type). Tenant-aware ID prefixing. Post-hook updates `actual_delivery_system_type` from interactions. | `UDL.NEWSLETTER` |
| `wrk_newsletter_interaction` | `(tenant_code, code)` | 5 seed lookups (interaction type, delivery system, recipient type, click type, block type). Device type classification from user agent. | `UDL.NEWSLETTER_INTERACTION` |
| `wrk_newsletter_category` | `(tenant_code, code)` | Data source derivation. Null-safe defaults. | `UDL.NEWSLETTER_CATEGORY` |
| `pipeline_complete` | N/A (sentinel view) | Depends on all 3 wrk_* + snap_newsletter. Post-hooks trigger publish and archive. | N/A |

### 6.4 Snapshot (1 snapshot, SCD-2 check strategy)

| Snapshot | Source | Strategy | Check Columns | Published As |
|----------|--------|----------|---------------|--------------|
| `snap_newsletter` | `wrk_newsletter` | `check` | `hash_value`, `actual_delivery_system_type` | `UDL.NEWSLETTER_SCD2` |

---

## 7. Testing Strategy

### 7.1 Test Summary (51 automated tests)

| Test Type | Count | Applied To |
|-----------|-------|------------|
| `not_null` | 18 | Source IDs, timestamps, mart primary keys, hash values, audit columns |
| `unique` | 6 | Source IDs, mart primary keys |
| `accepted_values` | 6 | Event types, data source codes, device types |
| `dbt_utils.unique_combination_of_columns` | 3 | Composite uniqueness on `(tenant_code, code)` for all 3 marts |
| Schema contracts (`contract: enforced`) | 3 | All 3 mart models — validates column names + data types at build time |

### 7.2 Schema Contracts

All mart models enforce `contract: enforced: true` with `on_schema_change: fail`:
- Every column's name and data type is validated against the YAML definition at build time
- If a column is added, removed, or changes type, the build fails immediately
- Prevents silent schema drift between pipeline changes

### 7.3 Data Quality Packages

- **dbt_utils** — `unique_combination_of_columns`, utility macros
- **dbt_expectations** — Available for additional data quality assertions

---

## 8. Operational Features

### 8.1 Audit & Logging

Every model execution is tracked with 5 audit columns:

| Column | Description |
|--------|-------------|
| `dbt_loaded_at` | Timestamp when dbt loaded the record |
| `dbt_run_id` | dbt invocation ID (unique per run) |
| `dbt_batch_id` | MD5(invocation_id + model_name) — unique per model per run |
| `dbt_source_model` | Name of the dbt model that produced the record |
| `dbt_environment` | Target environment (dev / prod) |

**Run-level logging:**

| Event | Table | Hook |
|-------|-------|------|
| Run starts | `DBT_RUN_LOG` (INSERT with status=RUNNING) | `on-run-start` |
| Each model completes | `DBT_MODEL_LOG` (INSERT with rows_affected, timing) | Model `post-hook` |
| Failed/skipped models | `DBT_MODEL_LOG` (INSERT with error_message) | `on-run-end` |
| Run ends | `DBT_RUN_LOG` (UPDATE with duration, final status, counts) | `on-run-end` |

### 8.2 Monitoring & Dashboards

**28 monitoring views** in `DBT_EXECUTION_RUN_STATS`:
- Run success rate, duration trends, consecutive failures
- Model execution history, row count trends
- Cost attribution by model (via `SNOWFLAKE.ACCOUNT_USAGE`)
- Test pass rates and coverage by type
- Data freshness by entity

**30+ dashboard tile queries** organized into sections:
- A: Executive overview (pipeline health scorecard)
- B: Automated testing metrics
- C: Developer productivity (model timing, incremental efficiency)
- D: AI-assisted development (reserved)
- E: Failure recovery (error analysis, retry history)
- F: Performance improvements (execution trends)
- G: Data quality validation
- H: Data reconciliation (source → staging → mart completeness)
- I: Snapshot consistency checks

### 8.3 Smart Retry (Snowflake Native dbt)

Snowflake Native dbt lacks `dbt retry` and `--state` support. Custom retry infrastructure compensates:

| Component | Purpose |
|-----------|---------|
| `DBT_MODEL_MANIFEST` | Static registry of all expected dbt models with dependencies |
| `V_RETRY_CANDIDATES` | View showing models that need retry for a given run |
| `PRC_DBT_SMART_RETRY` | Builds `--select` clause from failed/skipped models, optionally executes |
| `PRC_DBT_FULL_RUN_WITH_RETRY` | Wrapper: full build + automatic retry up to N times |
| `PRC_DBT_SMART_RETRY_V2` | Enhanced retry using artifact-based ground-truth from `run_results.json` |

### 8.4 Artifact-Based Logging

Uses `SYSTEM$LOCATE_DBT_ARTIFACTS()` to capture `manifest.json` and `run_results.json` after every execution:

| Component | Purpose |
|-----------|---------|
| `DBT_TEST_RESULTS` | Granular test outcomes (test type, column, object, pass/fail, failure count) |
| `DBT_BUILD_RESULTS` | Model/snapshot/seed execution outcomes (status, timing, rows affected) |
| `PRC_DBT_LOG_ARTIFACTS` | Parses artifacts from any prior run into logging tables |
| `PRC_DBT_EXECUTE_AND_LOG_BUILD` | Run `dbt build` + automatically capture artifacts |
| `PRC_DBT_EXECUTE_AND_LOG_TESTS` | Run `dbt test` + automatically capture test results |

---

## 9. Configuration & Variables

### 9.1 Runtime Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `is_full_load` | `false` | When `true`, staging models UNION raw + archive tables for complete rebuild |
| `entity_specific_full_load` | `'none'` | Set to `'newsletter'`, `'interaction'`, or `'category'` for per-entity full load |
| `data_process_end_time` | `'9999-12-31 23:59:59'` | Processing window boundary — filters source data and archival scope |
| `enable_publish` | `true` | Toggle atomic clone+swap publish to UDL |
| `enable_archive` | `true` | Toggle raw data archival after publish |
| `enable_audit_logging` | `true` | Toggle run/model logging to audit tables |
| `enable_row_count_tracking` | `true` | Toggle row count capture in model post-hooks |
| `default_null_timestamp` | `'2000-01-01 00:00:00'` | Sentinel value for NULL timestamps in hash computation |

### 9.2 Override at Runtime

```sql
-- Full load with archival disabled
EXECUTE DBT PROJECT ... ARGS = 'build --target dev --vars ''{is_full_load: true, enable_archive: false}'''

-- Entity-specific full load
EXECUTE DBT PROJECT ... ARGS = 'build --target dev --vars ''{entity_specific_full_load: newsletter}'''

-- Skip publish (testing only)
EXECUTE DBT PROJECT ... ARGS = 'build --target dev --vars ''{enable_publish: false}'''
```

---

## 10. Execution & Deployment

### 10.1 Setup Order (One-Time)

| Step | Script | Creates |
|------|--------|---------|
| 1 | `account_bootstrap.sql` | Database, 5 schemas, 6 source/archive tables, 3 UDL fact tables, NEWSLETTER_HIST, sample data, access integration |
| 2 | `audit_setup.sql` | `DBT_RUN_LOG`, `DBT_MODEL_LOG`, 4 summary views |
| 3 | `publish_archive_setup.sql` | Merge helper + publish + archive procedures, NEWSLETTER_HIST audit columns |
| 4 | `retry_setup.sql` | Model manifest, retry views, retry procedures |
| 5 | `test_logging_setup.sql` | Artifact stage, test/build logging tables, artifact procedures, monitoring views |
| 6 | `dbt deps` | Install dbt_utils + dbt_expectations packages |
| 7 | `dbt seed` | Load 6 reference CSV tables |
| 8 | `dbt build --full-refresh` | Initial full build (creates all tables as permanent) |
| 9 | `monitoring_queries.sql` | 28 monitoring views (requires dbt tables to exist) |
| 10 | `dashboard_queries.sql` | 30+ dashboard tile queries |

### 10.2 Regular Execution

```sql
-- Standard incremental build (processes only new/changed data)
EXECUTE DBT PROJECT ... ARGS = 'build --target dev'

-- Build with auto-retry (up to 2 retries on failure)
CALL UDL_BATCH_PROCESS.PRC_DBT_FULL_RUN_WITH_RETRY(
    '@my_git_stage', '/newsletter_poc', 'dev', 2, NULL
);

-- Build with artifact logging (captures test + model results)
CALL UDL_BATCH_PROCESS.PRC_DBT_EXECUTE_AND_LOG_BUILD(
    'FROM @my_git_stage PROJECT_ROOT=''/newsletter_poc''',
    '--target dev'
);
```

### 10.3 DAG Execution Order

```
dbt seed (6 reference tables)
    │
    ├── stg_newsletter ──────────────────┐
    ├── stg_newsletter_recipient ────────┤
    ├── stg_newsletter_interaction ──────┤
    ├── stg_newsletter_interaction_summary┤
    └── stg_newsletter_category ─────────┤
                                         │
    int_newsletter_joined (ephemeral) ◄──┤
                                         │
    ├── wrk_newsletter ◄─────────────────┤
    ├── wrk_newsletter_interaction ◄─────┤
    └── wrk_newsletter_category ◄────────┘
                │
    snap_newsletter ◄────────────────────┘
                │
    pipeline_complete (sentinel)
        ├── post-hook: publish_to_target()
        └── post-hook: archive_raw_data()
```

---

## 11. Data Validation

### 11.1 Validation Script

A comprehensive 6-round validation script (`setup/dataload_pattern_tests.sql`) tests all data loading patterns with before/after queries and expected results:

| Round | Pattern Tested | Validates |
|-------|---------------|-----------|
| 1 | Initial Full Load | 5 source rows → 4 deduped (ROW_NUMBER ranking), audit columns populated |
| 2 | Incremental Merge + SCD-2 | Modified record updated in-place, new record inserted, snapshot creates historical version |
| 3 | Delete Handling | NEWSLETTER_DELETED → is_deleted=TRUE, SCD-2 captures pre/post-delete versions |
| 4 | Hash-Based Dedup (No-Op) | Re-run with no new data → no updates (hash match prevents re-processing) |
| 5 | Publish Verification | UDL.* matches DBT_UDL.WRK_*, NEWSLETTER_HIST accumulates each build |
| 6 | Full Load + Archive | Records split across raw/archive → UNION ALL produces same complete result |

### 11.2 Data Reconciliation Queries

Dashboard Section H provides 7 reconciliation queries:
- Source → Staging → Mart row count pipeline (per entity)
- Unique key count comparison across layers
- Orphan detection (interactions without matching newsletters)
- Category join completeness
- Cross-entity key overlap analysis
- Snapshot consistency vs. current mart state
- Overall pipeline health score

---

## 12. Feature Comparison: Scala Pipeline vs. dbt Pipeline

| Feature | Scala Pipeline | dbt Pipeline |
|---------|---------------|--------------|
| Language | Scala/Snowpark | SQL + Jinja |
| Execution | Airflow orchestrated | Snowflake Native dbt |
| Incremental processing | Custom merge logic | dbt `incremental` materialization |
| Deduplication | Custom hash comparison | Same MD5 hash, dbt-native dedup |
| SCD-2 history | Manual INSERT/UPDATE | dbt snapshot (check strategy) |
| Schema validation | Manual | `contract: enforced: true` |
| Schema drift protection | None | `on_schema_change: fail` |
| Automated testing | None | 51 tests (not_null, unique, accepted_values, composite uniqueness) |
| Publish to UDL | DELETE + INSERT | Hybrid Merge+Clone (MERGE fact tables, CLONE SCD2) |
| Time Travel on UDL | Lost on each publish cycle | Preserved on fact tables (MERGE is DML, not DDL) |
| Audit trail | BATCH_RUN / PROCESS_RUN | DBT_RUN_LOG / DBT_MODEL_LOG |
| Run retry | Manual re-run | Smart retry (PRC_DBT_SMART_RETRY) |
| Monitoring | Manual queries | 28 views + 30+ dashboard queries |
| Code documentation | Inline comments | YAML descriptions, persist_docs, data_flow.md |
| Version control | Git | Git + Snowflake Git integration |
| Dependency management | Manual ordering | dbt DAG (automatic via `ref()` / `source()`) |

---

## 13. Validation Checklist

For customer sign-off, validate the following:

### Data Completeness
- [ ] All source records are processed (no data loss between source → staging → mart)
- [ ] Deduplication produces correct results (latest version wins per tenant+code)
- [ ] All reference code lookups map correctly (status, recipient type, etc.)
- [ ] Full-load mode reconstructs complete dataset from raw + archive

### Data Accuracy
- [ ] Hash values match expected computation (MD5 over all business columns)
- [ ] Channel flags correctly extracted from JSON (send_as_email, send_as_sms, etc.)
- [ ] Tenant code correctly extracted from nested HEADER JSON
- [ ] Delete events correctly set is_deleted = TRUE
- [ ] Device type classification matches expected categories

### Incremental Behavior
- [ ] Modified records update in-place (MERGE update, not duplicate)
- [ ] New records insert correctly (MERGE insert)
- [ ] Unchanged records are skipped (hash-based dedup)
- [ ] Watermark (`created_datetime >= MAX(dbt_loaded_at)`) captures all relevant records

### SCD-2 History
- [ ] Changed records create new version (dbt_valid_to set on old, new row inserted)
- [ ] Unchanged records maintain current version (no spurious history)
- [ ] Hard deletes are tracked (invalidate_hard_deletes = True)

### Publish & Archive
- [ ] UDL tables match DBT_UDL work tables after publish (row counts + data content)
- [ ] MERGE correctly inserts new rows, updates changed rows, deletes removed rows
- [ ] Time Travel works on UDL fact tables after publish (`SELECT * FROM UDL.NEWSLETTER AT(OFFSET => -60)`)
- [ ] NEWSLETTER_HIST accumulates rows with each build
- [ ] NEWSLETTER_SCD2 reflects complete snapshot history
- [ ] Archive tables receive records after publish
- [ ] Source tables are purged after archival
- [ ] All operations are atomic (transaction-scoped)

### Operational
- [ ] Audit columns populated on all records (dbt_loaded_at, dbt_run_id, etc.)
- [ ] Run log captures start/end/duration/status
- [ ] Failed models are logged with error messages
- [ ] Retry mechanism identifies and re-runs failed models
- [ ] Monitoring views return correct data

---

*Document generated for project version 1.0.0. Refer to `setup/dataload_pattern_tests.sql` for hands-on validation.*
