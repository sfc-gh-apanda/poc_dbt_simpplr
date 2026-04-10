-- ═══════════════════════════════════════════════════════════════════════════════
-- DATA LOAD PATTERN VALIDATION SCRIPT
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Tests all data loading patterns used in the newsletter dbt pipeline:
--
--   ROUND 1: Initial Full Load (first dbt build)
--   ROUND 2: Incremental Merge + SCD-2 (modified record + new record)
--   ROUND 3: Delete Handling (NEWSLETTER_DELETED event → is_deleted flag)
--   ROUND 4: Hash-Based Dedup (same data re-arrives, no change)
--   ROUND 5: Publish Verification (HIST-as-master + MERGE → UDL)
--   ROUND 6: Full Load with Archive Tables
--   ROUND 7: Customer-Driven Reprocessing (archived record re-pulled via REPROCESS_REQUEST)
--
-- How to use:
--   1. Run account_bootstrap.sql through test_logging_setup.sql first
--   2. Run dbt deps + dbt seed (one-time, before Round 1)
--   3. Execute each ROUND in order — run the BEFORE queries, then dbt build,
--      then the AFTER queries.
--   4. Each round has clear EXPECTED results documented.
--
-- Airflow vars:
--   batch_run_id            — unique identifier from Airflow DAG run (integer)
--   data_process_end_time   — batch end boundary (TIMESTAMP)
--   Note: start_time is no longer used — raw tables are self-pruning (archive
--         removes processed records), so staging reads everything up to end_time.
--
-- Database: COMMON_TENANT_DEV
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE COMMON_TENANT_DEV;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  PRE-REQUISITE: Install packages + load seed/reference data (once)       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Install dbt_utils and dbt_expectations packages
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'deps';

-- Load reference/seed tables (status, recipient_type, block_type, etc.)
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'seed --target dev';


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 1: INITIAL FULL LOAD                                             ║
-- ║  Pattern: Full table build (no incremental), dedup via ROW_NUMBER        ║
-- ║  End boundary: 2026-03-24 (covers all bootstrap data)                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- BEFORE: Verify source data from account_bootstrap.sql
-- ─────────────────────────────────────────────────────────────────────────────

-- 1A. Source row counts
SELECT 'VW_ENL_NEWSLETTER' AS source_table, COUNT(*) AS row_count
FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
UNION ALL
SELECT 'VW_ENL_NEWSLETTER_INTERACTION', COUNT(*)
FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
UNION ALL
SELECT 'VW_ENL_NEWSLETTER_CATEGORY', COUNT(*)
FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY;
-- EXPECTED:
--   VW_ENL_NEWSLETTER             → 5 (4 unique + 1 duplicate of newsletter 1)
--   VW_ENL_NEWSLETTER_INTERACTION → 4
--   VW_ENL_NEWSLETTER_CATEGORY    → 3

-- 1B. Verify the duplicate pair (same code+tenant, different kafka_timestamp)
SELECT
    domain_payload:id::STRING     AS code,
    TRY_PARSE_JSON(header:tenant_info):accountId::STRING AS tenant,
    type,
    kafka_timestamp,
    domain_payload:name::STRING   AS name
FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE domain_payload:id::STRING = '85db08a2-3579-49b8-b4a4-1d80fd9021a7'
ORDER BY kafka_timestamp;
-- EXPECTED: 2 rows — NEWSLETTER_CREATED (earlier) and NEWSLETTER_MODIFIED (later)
--   The MODIFIED version ("Copy of test") should win after ROW_NUMBER ranking

-- 1C. Confirm target tables do NOT exist yet
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DBT_UDL' AND TABLE_NAME IN ('WRK_NEWSLETTER', 'WRK_NEWSLETTER_INTERACTION', 'WRK_NEWSLETTER_CATEGORY');
-- EXPECTED: 0 rows (tables don't exist before first build)


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Run first dbt build (batch_run_id=1001, end boundary covers all bootstrap data)
-- ─────────────────────────────────────────────────────────────────────────────
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1001,
--         "data_process_end_time": "2026-03-24 00:00:00"
--     }''';


-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER: Validate initial load results
-- ─────────────────────────────────────────────────────────────────────────────

-- 1D. Work table row counts (dedup should have reduced newsletters from 5→4)
SELECT 'WRK_NEWSLETTER' AS table_name, COUNT(*) AS row_count
FROM DBT_UDL.WRK_NEWSLETTER
UNION ALL
SELECT 'WRK_NEWSLETTER_INTERACTION', COUNT(*)
FROM DBT_UDL.WRK_NEWSLETTER_INTERACTION
UNION ALL
SELECT 'WRK_NEWSLETTER_CATEGORY', COUNT(*)
FROM DBT_UDL.WRK_NEWSLETTER_CATEGORY;
-- EXPECTED:
--   WRK_NEWSLETTER             → 4 (5 source rows deduplicated to 4 unique newsletters)
--   WRK_NEWSLETTER_INTERACTION → 4
--   WRK_NEWSLETTER_CATEGORY    → 3

-- 1E. Verify the duplicate was resolved — only the MODIFIED version survived
SELECT code, name, hash_value, dbt_loaded_at
FROM DBT_UDL.WRK_NEWSLETTER
WHERE code = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';
-- EXPECTED: 1 row, name = 'Copy of test' (the MODIFIED version, not 'test')

-- 1F. NEWSLETTER_HIST — initial state (all records active, no historical versions)
SELECT
    tenant_code || '|' || code AS business_key,
    name,
    hash_value,
    active_flag,
    active_date,
    inactive_date,
    IFF(active_flag, 'ACTIVE', 'INACTIVE') AS version_status
FROM UDL.NEWSLETTER_HIST
ORDER BY business_key, active_date;
-- EXPECTED: 4 rows, all with active_flag = TRUE (all ACTIVE, no history yet)

-- 1G. Audit columns populated
SELECT code, batch_run_id, dbt_loaded_at, dbt_run_id, dbt_batch_id, dbt_source_model, dbt_environment
FROM DBT_UDL.WRK_NEWSLETTER
LIMIT 3;
-- EXPECTED: All audit columns should be non-null; batch_run_id = 0 when run without Airflow

-- 1H. Publish verification — UDL tables should match DBT_UDL
SELECT 'UDL.NEWSLETTER' AS table_name, COUNT(*) AS row_count FROM UDL.NEWSLETTER
UNION ALL
SELECT 'UDL.NEWSLETTER_INTERACTION', COUNT(*) FROM UDL.NEWSLETTER_INTERACTION
UNION ALL
SELECT 'UDL.NEWSLETTER_CATEGORY', COUNT(*) FROM UDL.NEWSLETTER_CATEGORY
UNION ALL
SELECT 'UDL.NEWSLETTER_HIST', COUNT(*) FROM UDL.NEWSLETTER_HIST;
-- EXPECTED: NEWSLETTER=4, INTERACTION=4, CATEGORY=3, HIST=4

-- 1I. Seed tables loaded
SELECT 'ref_newsletter_status' AS seed, COUNT(*) AS rows FROM DBT_UDL.REF_NEWSLETTER_STATUS
UNION ALL SELECT 'ref_newsletter_interaction_type', COUNT(*) FROM DBT_UDL.REF_NEWSLETTER_INTERACTION_TYPE
UNION ALL SELECT 'ref_newsletter_delivery_system_type', COUNT(*) FROM DBT_UDL.REF_NEWSLETTER_DELIVERY_SYSTEM_TYPE
UNION ALL SELECT 'ref_newsletter_recipient_type', COUNT(*) FROM DBT_UDL.REF_NEWSLETTER_RECIPIENT_TYPE
UNION ALL SELECT 'ref_newsletter_click_type', COUNT(*) FROM DBT_UDL.REF_NEWSLETTER_CLICK_TYPE
UNION ALL SELECT 'ref_newsletter_block_type', COUNT(*) FROM DBT_UDL.REF_NEWSLETTER_BLOCK_TYPE;
-- EXPECTED: All seed tables should have rows (exact counts depend on CSV content)


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 2: INCREMENTAL MERGE + SCD-2                                     ║
-- ║  Pattern: Modified record → MERGE updates in-place, SCD-2 creates       ║
-- ║           new version. New record → INSERT via MERGE.                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- BEFORE: Capture current state
-- ─────────────────────────────────────────────────────────────────────────────

-- 2A. Current state of newsletter "Weekly Update" (will be modified)
SELECT code, name, subject, status_code, hash_value, dbt_loaded_at
FROM DBT_UDL.WRK_NEWSLETTER
WHERE code = '202a20ae-ba80-4d69-ad0f-46febe2e293c';
-- EXPECTED: 1 row, subject = 'Your weekly digest'

-- 2B. Current row counts
SELECT COUNT(*) AS wrk_newsletter_count FROM DBT_UDL.WRK_NEWSLETTER;
-- EXPECTED: 4

-- 2C. Current NEWSLETTER_HIST state
SELECT COUNT(*) AS hist_total,
       SUM(IFF(active_flag, 1, 0)) AS active_versions,
       SUM(IFF(NOT active_flag, 1, 0)) AS inactive_versions
FROM UDL.NEWSLETTER_HIST;
-- EXPECTED: hist_total=4, active_versions=4, inactive_versions=0

-- 2D. Current NEWSLETTER_HIST count
SELECT COUNT(*) AS hist_count FROM UDL.NEWSLETTER_HIST;
-- EXPECTED: 4 (from Round 1 publish)


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Insert MODIFIED newsletter + BRAND NEW newsletter
-- ─────────────────────────────────────────────────────────────────────────────

-- 2E. Modified: "Weekly Update" — change subject and name (same code+tenant → triggers MERGE update)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12250, 12245,
    PARSE_JSON('{
        "id": "202a20ae-ba80-4d69-ad0f-46febe2e293c",
        "name": "Weekly Update v2",
        "subject": "Your updated weekly digest",
        "status": "sent",
        "channels": {
            "email": {"type": "email", "sender_address": "updates@dev.simpplr.xyz", "domain": "dev.simpplr.xyz"},
            "sms": {"type": "sms"},
            "msTeams": {"type": "msTeams"}
        },
        "recipients": [
            {"count": 175, "id": "aaa11111-1111-1111-1111-111111111111", "name": "All Employees", "type": "audience"}
        ],
        "category": {"id": "cat-001-uuid-here-1234567890ab"},
        "template": {"id": "tmpl-002-uuid-here-1234567890ab", "name": "Weekly Digest"},
        "theme": {"id": "thm-002-uuid-here-12345678901234", "name": "corporate theme"},
        "creator_id": "usr-creator-0001-uuid-12345678",
        "modifier_id": "usr-modifier-0001-uuid-1234567",
        "created_at": "2026-03-20T09:00:00.000Z",
        "modified_at": "2026-03-25T08:00:00.000Z",
        "sent_at": "2026-03-20T10:30:00.000Z",
        "send_at": "2026-03-20T10:00:00.000Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": true,
        "reply_to_address": "updates@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_MODIFIED',
    '202a20ae-ba80-4d69-ad0f-46febe2e293c',
    NULL,
    34290,
    '2026-03-25 08:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-25 08:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"202a20ae-ba80-4d69-ad0f-46febe2e293c","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-25 08:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- 2F. New: Brand new newsletter (new code+tenant → triggers MERGE insert)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12251, 12246,
    PARSE_JSON('{
        "id": "new-nl-round2-uuid-1234567890ab",
        "name": "Q1 Report",
        "subject": "Quarterly business review",
        "status": "draft",
        "channels": {"email": {"type": "email", "sender_address": "reports@dev.simpplr.xyz"}},
        "recipients": [{"count": 300, "id": "exec-001-uuid-here-1234567890ab", "name": "Executives", "type": "audience"}],
        "template": {"id": "tmpl-005-uuid-here-1234567890ab"},
        "theme": {"id": "thm-005-uuid-here-12345678901234"},
        "creator_id": "usr-creator-0004-uuid-12345678",
        "modifier_id": "usr-creator-0004-uuid-12345678",
        "created_at": "2026-03-25T09:00:00.000Z",
        "modified_at": "2026-03-25T09:00:00.000Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": false,
        "reply_to_address": "reports@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_CREATED',
    'new-nl-round2-uuid-1234567890ab',
    NULL,
    34291,
    '2026-03-25 09:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-25 09:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"new-nl-round2-uuid-1234567890ab","eventType":"NEWSLETTER_CREATED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-25 09:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Run dbt build (batch_run_id=1002, end boundary covers Round 2 inserts)
--   Modified newsletter created_datetime = 2026-03-25 08:05:00
--   New newsletter created_datetime      = 2026-03-25 09:05:00
-- ─────────────────────────────────────────────────────────────────────────────
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1002,
--         "data_process_end_time": "2026-03-26 00:00:00"
--     }''';


-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER: Validate incremental merge + SCD-2
-- ─────────────────────────────────────────────────────────────────────────────

-- 2G. Delta: wrk_newsletter should have ONLY new/changed records (NOT the full 5)
SELECT code, name, subject, hash_value, dbt_loaded_at
FROM DBT_UDL.WRK_NEWSLETTER;
-- EXPECTED: 2 rows — the MODIFIED "Weekly Update v2" + the NEW "Q1 Report"
--   (unchanged newsletters are NOT in the wrk table)

-- 2H. New newsletter in delta
SELECT code, name, subject
FROM DBT_UDL.WRK_NEWSLETTER
WHERE code = 'new-nl-round2-uuid-1234567890ab';
-- EXPECTED: 1 row, name = 'Q1 Report'

-- 2I. UDL.NEWSLETTER should have 5 total (4 existing + 1 new, 1 updated in-place)
SELECT COUNT(*) AS udl_newsletter_count FROM UDL.NEWSLETTER;
-- EXPECTED: 5

-- 2J. NEWSLETTER_HIST: "Weekly Update" should have 2 versions — old (inactive) + new (active)
SELECT
    tenant_code || '|' || code AS business_key,
    name,
    subject,
    hash_value,
    active_flag,
    active_date,
    inactive_date,
    IFF(active_flag, 'ACTIVE', 'INACTIVE') AS version_status
FROM UDL.NEWSLETTER_HIST
WHERE code = '202a20ae-ba80-4d69-ad0f-46febe2e293c'
ORDER BY active_date;
-- EXPECTED: 2 rows:
--   Row 1: name='Weekly Update',    subject='Your weekly digest',         version_status='INACTIVE'
--   Row 2: name='Weekly Update v2', subject='Your updated weekly digest', version_status='ACTIVE'

-- 2K. NEWSLETTER_HIST: Total should have 4 (round 1 originals) + 2 (round 2 delta) = 6 rows
--     1 of the round-1 rows was deactivated (Weekly Update), so: 3 active originals + 1 inactive original + 2 new active
SELECT
    COUNT(*)                              AS hist_total,
    SUM(IFF(active_flag, 1, 0))           AS active_versions,
    SUM(IFF(NOT active_flag, 1, 0))       AS inactive_versions
FROM UDL.NEWSLETTER_HIST;
-- EXPECTED: hist_total=6, active_versions=5, inactive_versions=1

-- 2L. UDL.NEWSLETTER should have exactly the active HIST records
SELECT
    (SELECT COUNT(*) FROM UDL.NEWSLETTER) AS udl_count,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_HIST WHERE ACTIVE_FLAG = TRUE) AS hist_active_count,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER) = (SELECT COUNT(*) FROM UDL.NEWSLETTER_HIST WHERE ACTIVE_FLAG = TRUE) AS counts_match;
-- EXPECTED: udl_count=5, hist_active_count=5, counts_match=TRUE


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 3: DELETE HANDLING                                                ║
-- ║  Pattern: NEWSLETTER_DELETED event → is_deleted=TRUE, SCD-2 captures    ║
-- ║  End boundary: 2026-03-27 (covers delete event)                          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- BEFORE: Capture current state of "Scheduled Monthly" (will be deleted)
-- ─────────────────────────────────────────────────────────────────────────────

-- 3A. Current state — should be active
SELECT code, name, is_deleted, active_flag, hash_value
FROM DBT_UDL.WRK_NEWSLETTER
WHERE code = 'sch-nl-001-uuid-here-1234567890ab';
-- EXPECTED: is_deleted=FALSE, active_flag=TRUE


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Insert a NEWSLETTER_DELETED event for "Scheduled Monthly"
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12252, 12247,
    PARSE_JSON('{
        "id": "sch-nl-001-uuid-here-1234567890ab",
        "name": "Scheduled Monthly",
        "subject": "March newsletter",
        "status": "scheduled",
        "channels": {
            "email": {"type": "email", "sender_address": "monthly@dev.simpplr.xyz"},
            "slack": {"type": "slack"}
        },
        "recipients": [{"count": 200, "id": "all-001-uuid-here-1234567890ab", "name": "Everyone", "type": "audience"}],
        "template": {"id": "tmpl-004-uuid-here-1234567890ab"},
        "theme": {"id": "thm-004-uuid-here-12345678901234"},
        "creator_id": "usr-creator-0003-uuid-12345678",
        "modifier_id": "usr-creator-0003-uuid-12345678",
        "created_at": "2026-03-22T16:00:00.000Z",
        "modified_at": "2026-03-26T10:00:00.000Z",
        "send_at": "2026-03-25T09:00:00.000Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": true,
        "reply_to_address": "monthly@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_DELETED',
    'sch-nl-001-uuid-here-1234567890ab',
    NULL,
    34295,
    '2026-03-26 10:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-26 10:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"sch-nl-001-uuid-here-1234567890ab","eventType":"NEWSLETTER_DELETED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-26 10:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Run dbt build (batch_run_id=1003, end boundary covers delete event)
--   Delete event created_datetime = 2026-03-26 10:05:00
-- ─────────────────────────────────────────────────────────────────────────────
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1003,
--         "data_process_end_time": "2026-03-27 00:00:00"
--     }''';


-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER: Validate delete handling
-- ─────────────────────────────────────────────────────────────────────────────

-- 3B. Delta wrk_newsletter should contain ONLY the changed record
SELECT code, name, is_deleted, active_flag, hash_value
FROM DBT_UDL.WRK_NEWSLETTER
WHERE code = 'sch-nl-001-uuid-here-1234567890ab';
-- EXPECTED: is_deleted=TRUE (derived from event type NEWSLETTER_DELETED)
--   The hash_value should DIFFER from Round 2 (is_deleted changed)

-- 3C. UDL.NEWSLETTER should still have 5 records (delete just updates the row)
SELECT COUNT(*) AS udl_newsletter_count FROM UDL.NEWSLETTER;
-- EXPECTED: 5

-- 3D. NEWSLETTER_HIST: "Scheduled Monthly" should have 2 versions (pre-delete + post-delete)
SELECT
    name,
    is_deleted,
    hash_value,
    active_flag,
    active_date,
    inactive_date,
    IFF(active_flag, 'ACTIVE', 'INACTIVE') AS version_status
FROM UDL.NEWSLETTER_HIST
WHERE code = 'sch-nl-001-uuid-here-1234567890ab'
ORDER BY active_date;
-- EXPECTED: 2 rows:
--   Row 1: is_deleted=FALSE, version_status='INACTIVE' (old version deactivated)
--   Row 2: is_deleted=TRUE,  version_status='ACTIVE'   (delete event version)


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 4: HASH-BASED DEDUP (NO-OP INCREMENTAL)                          ║
-- ║  Pattern: Same data arrives again → hash matches → skipped by dedup      ║
-- ║  End boundary: 2026-03-28 (no new source data since last run)            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- BEFORE: Capture hash and dbt_loaded_at for a record
-- ─────────────────────────────────────────────────────────────────────────────

-- 4A. UDL.NEWSLETTER state before no-op run (wrk is delta-only, check published table)
SELECT code, name, hash_value
FROM UDL.NEWSLETTER
WHERE code = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';
-- NOTE: Save hash_value — UDL.NEWSLETTER should NOT change after next build (no delta)


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Run dbt build WITHOUT inserting any new source data (batch_run_id=1004)
--   No new records in raw → staging reads existing raw records → hash dedup → empty delta
-- ─────────────────────────────────────────────────────────────────────────────
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1004,
--         "data_process_end_time": "2026-03-28 00:00:00"
--     }''';


-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER: Verify no changes (hash dedup prevented unnecessary updates)
-- ─────────────────────────────────────────────────────────────────────────────

-- 4B. wrk_newsletter should be EMPTY (no changes = no delta)
SELECT COUNT(*) AS wrk_newsletter_count FROM DBT_UDL.WRK_NEWSLETTER;
-- EXPECTED: 0 (hash dedup against UDL.NEWSLETTER filters out all unchanged records)

-- 4C. NEWSLETTER_HIST should have NO new versions
SELECT
    COUNT(*)                              AS hist_total,
    SUM(IFF(active_flag, 1, 0))           AS active_versions,
    SUM(IFF(NOT active_flag, 1, 0))       AS inactive_versions
FROM UDL.NEWSLETTER_HIST;
-- EXPECTED: Same counts as end of Round 3 (no new history created for unchanged data)


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 5: PUBLISH VERIFICATION (HIST-AS-MASTER)                        ║
-- ║  Pattern: wrk delta → NEWSLETTER_HIST → UDL.NEWSLETTER + MERGE others  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER any dbt build (publish runs automatically via pipeline_complete post-hook)
-- ─────────────────────────────────────────────────────────────────────────────

-- 5A. UDL.NEWSLETTER = active NEWSLETTER_HIST records
SELECT
    'NEWSLETTER'             AS entity,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_HIST WHERE ACTIVE_FLAG = TRUE) AS hist_active_count,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER)                               AS udl_count,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_HIST WHERE ACTIVE_FLAG = TRUE) =
    (SELECT COUNT(*) FROM UDL.NEWSLETTER)                               AS match
UNION ALL
SELECT
    'NEWSLETTER_INTERACTION',
    NULL,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_INTERACTION),
    TRUE
UNION ALL
SELECT
    'NEWSLETTER_CATEGORY',
    NULL,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_CATEGORY),
    TRUE;
-- EXPECTED: NEWSLETTER MATCH = TRUE, all entities have expected row counts

-- 5B. Data content parity: spot-check a record in UDL.NEWSLETTER vs NEWSLETTER_HIST
SELECT 'NEWSLETTER_HIST' AS source_table, code, name, subject, hash_value, active_flag
FROM UDL.NEWSLETTER_HIST WHERE code = '202a20ae-ba80-4d69-ad0f-46febe2e293c' AND ACTIVE_FLAG = TRUE
UNION ALL
SELECT 'NEWSLETTER', code, name, subject, hash_value, TRUE
FROM UDL.NEWSLETTER WHERE code = '202a20ae-ba80-4d69-ad0f-46febe2e293c';
-- EXPECTED: Both rows identical (UDL.NEWSLETTER derived from active HIST)

-- 5C. NEWSLETTER_HIST accumulation: should grow with each build
SELECT
    batch_run_id,
    published_by_run_id,
    published_at,
    COUNT(*) AS rows_published
FROM UDL.NEWSLETTER_HIST
GROUP BY batch_run_id, published_by_run_id, published_at
ORDER BY published_at;
-- EXPECTED: One group per dbt build, each with the newsletter count at that point
--   Round 1: 4 rows, Round 2: 5 rows, Round 3: 5 rows, Round 4: 5 rows = 19 total
--   batch_run_id should be consistent across all records in each publish event

-- 5D. End-to-end traceability: published_by_run_id on INTERACTION and CATEGORY
SELECT 'NEWSLETTER_INTERACTION' AS entity, published_by_run_id, published_at, COUNT(*) AS rows_count
FROM UDL.NEWSLETTER_INTERACTION
GROUP BY published_by_run_id, published_at
UNION ALL
SELECT 'NEWSLETTER_CATEGORY', published_by_run_id, published_at, COUNT(*)
FROM UDL.NEWSLETTER_CATEGORY
GROUP BY published_by_run_id, published_at
ORDER BY entity, published_at;
-- EXPECTED: published_by_run_id populated on all target tables (not just NEWSLETTER_HIST)

-- 5D. Pipeline complete sentinel view should show current state
SELECT * FROM DBT_UDL.PIPELINE_COMPLETE;
-- EXPECTED: 3 rows (one per entity) with current row counts


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 6: FULL LOAD WITH ARCHIVE TABLES                                 ║
-- ║  Pattern: is_full_load=true → staging UNIONs raw + archive tables        ║
-- ║  End boundary: 2026-03-28 (wide to cover all raw + archive data)          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- BEFORE: Move some records to archive to simulate post-archive state
-- ─────────────────────────────────────────────────────────────────────────────

-- 6A. Simulate archival: move the 2 oldest newsletters to archive
INSERT INTO SHARED_SERVICES_STAGING.ENL_NEWSLETTER_ARCHIVE
SELECT * FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE CREATED_DATETIME <= '2026-03-20 10:35:00.000'::TIMESTAMP_NTZ;

DELETE FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE CREATED_DATETIME <= '2026-03-20 10:35:00.000'::TIMESTAMP_NTZ;

-- 6B. Verify split: some in raw, some in archive
SELECT 'RAW' AS location, COUNT(*) AS cnt FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
UNION ALL
SELECT 'ARCHIVE', COUNT(*) FROM SHARED_SERVICES_STAGING.ENL_NEWSLETTER_ARCHIVE;
-- EXPECTED: RAW has fewer rows, ARCHIVE has the moved rows


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Run dbt build with is_full_load=true (batch_run_id=1006)
--   is_full_load=true → staging UNIONs raw + full archive tables
-- ─────────────────────────────────────────────────────────────────────────────
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1006,
--         "data_process_end_time": "2026-03-28 00:00:00",
--         "is_full_load": true
--     }''';


-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER: Validate full load with archive
-- ─────────────────────────────────────────────────────────────────────────────

-- 6C. Staging should have processed BOTH raw AND archive records
--     The wrk table count should remain the same (all unique records still present)
SELECT COUNT(*) AS wrk_newsletter_count FROM DBT_UDL.WRK_NEWSLETTER;
-- EXPECTED: 5 (same as before — full load just ensures completeness, not duplication)

-- 6D. Verify all original newsletters are still present (none lost due to archive split)
SELECT code, name
FROM DBT_UDL.WRK_NEWSLETTER
ORDER BY code;
-- EXPECTED: All 5 newsletters present (including the ones from archive)

-- 6E. Staging table should reflect UNION of raw + archive
SELECT COUNT(*) AS stg_newsletter_count FROM UDL.STG_NEWSLETTER;
-- EXPECTED: Total of raw + archive rows (before dedup by int_newsletter_joined)


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 7: CUSTOMER-DRIVEN REPROCESSING                                 ║
-- ║  Pattern: Customer queues record IDs in REPROCESS_REQUEST → staging     ║
-- ║           pulls matching records from archive → UNION with raw → delta  ║
-- ║           pipeline processes them normally → publish marks COMPLETED     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- BEFORE: Set up reprocessing scenario
--   Simulate a situation where records have been archived (moved out of raw)
--   and a customer requests reprocessing of specific records.
-- ─────────────────────────────────────────────────────────────────────────────

-- 7A. First, run the archive procedure to move processed records out of raw
--     (If Round 6 was run, some records may already be in archive.
--      This ensures the target newsletter IS in archive and NOT in raw.)
INSERT INTO SHARED_SERVICES_STAGING.ENL_NEWSLETTER_ARCHIVE
SELECT * FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE domain_payload:id::STRING = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';

DELETE FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE domain_payload:id::STRING = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';

-- Also archive one interaction for the same newsletter
INSERT INTO SHARED_SERVICES_STAGING.ENL_NEWSLETTER_INTERACTION_ARCHIVE
SELECT * FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
WHERE domain_payload:newsletter:id::STRING = '202a20ae-ba80-4d69-ad0f-46febe2e293c';

DELETE FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
WHERE domain_payload:newsletter:id::STRING = '202a20ae-ba80-4d69-ad0f-46febe2e293c';

-- 7B. Verify: target records are in archive, NOT in raw
SELECT 'RAW' AS location, COUNT(*) AS cnt
FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE domain_payload:id::STRING = '85db08a2-3579-49b8-b4a4-1d80fd9021a7'
UNION ALL
SELECT 'ARCHIVE', COUNT(*)
FROM SHARED_SERVICES_STAGING.ENL_NEWSLETTER_ARCHIVE
WHERE domain_payload:id::STRING = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';
-- EXPECTED: RAW=0, ARCHIVE>=1

-- 7C. Verify REPROCESS_REQUEST table is empty
SELECT COUNT(*) AS pending_requests FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST WHERE STATUS = 'PENDING';
-- EXPECTED: 0

-- 7D. Capture current UDL state before reprocessing
SELECT code, name, hash_value FROM UDL.NEWSLETTER
WHERE code = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';
-- NOTE: Save hash_value — it should match after reprocess if source data is unchanged

SELECT COUNT(*) AS hist_count FROM UDL.NEWSLETTER_HIST;
-- NOTE: Save count — will increase after reprocess (new active version in HIST)

SELECT COUNT(*) AS interaction_count FROM UDL.NEWSLETTER_INTERACTION
WHERE NEWSLETTER_CODE = '202a20ae-ba80-4d69-ad0f-46febe2e293c';
-- NOTE: Save count — interactions for this newsletter


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Customer submits reprocess requests
-- ─────────────────────────────────────────────────────────────────────────────

-- 7E. Customer requests reprocessing of a specific newsletter (archived)
INSERT INTO UDL_BATCH_PROCESS.REPROCESS_REQUEST
    (ENTITY_TYPE, RECORD_CODE, TENANT_CODE, REQUESTED_BY, REASON)
VALUES
    ('NEWSLETTER', '85db08a2-3579-49b8-b4a4-1d80fd9021a7', '260cd6ce-041e-4252-abcd-cd93f9804d4e',
     'customer_support', 'Source data corrected — need to reprocess');

-- 7F. Customer also requests reprocessing of related interactions
INSERT INTO UDL_BATCH_PROCESS.REPROCESS_REQUEST
    (ENTITY_TYPE, RECORD_CODE, TENANT_CODE, REQUESTED_BY, REASON)
VALUES
    ('NEWSLETTER_INTERACTION', 'int-001-uuid-here-1234567890ab', '260cd6ce-041e-4252-abcd-cd93f9804d4e',
     'customer_support', 'Reprocess interactions for corrected newsletter'),
    ('NEWSLETTER_INTERACTION', 'int-004-uuid-here-1234567890ab', '260cd6ce-041e-4252-abcd-cd93f9804d4e',
     'customer_support', 'Reprocess interactions for corrected newsletter');

-- 7G. Verify pending requests
SELECT * FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST ORDER BY REQUEST_ID;
-- EXPECTED: 3 rows — 1 NEWSLETTER + 2 NEWSLETTER_INTERACTION, all STATUS='PENDING'


-- ─────────────────────────────────────────────────────────────────────────────
-- ACTION: Run dbt build (batch_run_id=1007, end boundary covers current time)
--   The staging models will:
--     1. Read all remaining raw records (up to end_time) as usual
--     2. UNION with archive records matching REPROCESS_REQUEST (PENDING)
--   Hash dedup will then determine if any actual changes need processing.
-- ─────────────────────────────────────────────────────────────────────────────
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1007,
--         "data_process_end_time": "2026-03-30 00:00:00"
--     }''';


-- ─────────────────────────────────────────────────────────────────────────────
-- AFTER: Validate reprocessing results
-- ─────────────────────────────────────────────────────────────────────────────

-- 7H. Staging should have picked up the reprocess records from archive
SELECT COUNT(*) AS stg_newsletter_count FROM UDL.STG_NEWSLETTER
WHERE code = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';
-- EXPECTED: >= 1 (archive record pulled into staging via REPROCESS_REQUEST)

-- 7I. Check if wrk_newsletter has the reprocessed record
--     (depends on whether hash changed — if source data was corrected, hash differs → delta)
--     If source data is unchanged, hash dedup will filter it out (no-op reprocess)
SELECT code, name, hash_value
FROM DBT_UDL.WRK_NEWSLETTER
WHERE code = '85db08a2-3579-49b8-b4a4-1d80fd9021a7';
-- EXPECTED: If source data unchanged → 0 rows (hash dedup filtered it)
--           If source data corrected → 1 row (new version in delta)

-- 7J. Check interaction reprocessing
SELECT COUNT(*) AS stg_interaction_count FROM UDL.STG_NEWSLETTER_INTERACTION
WHERE code IN ('int-001-uuid-here-1234567890ab', 'int-004-uuid-here-1234567890ab');
-- EXPECTED: 2 (both interaction records pulled from archive)

-- 7K. REPROCESS_REQUEST should be empty (all COMPLETED and archived to _HIST)
SELECT COUNT(*) AS remaining_pending FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST WHERE STATUS = 'PENDING';
-- EXPECTED: 0 (publish procedure marked them COMPLETED and moved to _HIST)

-- 7L. REPROCESS_REQUEST_HIST should have the completed requests
SELECT REQUEST_ID, ENTITY_TYPE, RECORD_CODE, TENANT_CODE, STATUS,
       PROCESSED_BY_RUN_ID, PROCESSED_AT, BATCH_RUN_ID
FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST_HIST
ORDER BY REQUEST_ID;
-- EXPECTED: 3 rows — all STATUS='COMPLETED', BATCH_RUN_ID=1007,
--           PROCESSED_BY_RUN_ID = dbt invocation_id of the run

-- 7M. UDL tables should still be consistent
SELECT
    'NEWSLETTER'             AS entity,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_HIST WHERE ACTIVE_FLAG = TRUE) AS hist_active,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER)                               AS udl_count,
    (SELECT COUNT(*) FROM UDL.NEWSLETTER_HIST WHERE ACTIVE_FLAG = TRUE) =
    (SELECT COUNT(*) FROM UDL.NEWSLETTER)                               AS match;
-- EXPECTED: match = TRUE

-- 7N. End-to-end traceability: reprocessed records should carry batch_run_id=1007
SELECT code, name, batch_run_id, published_by_run_id, published_at
FROM UDL.NEWSLETTER_HIST
WHERE code = '85db08a2-3579-49b8-b4a4-1d80fd9021a7'
ORDER BY active_date;
-- EXPECTED: Latest active version has batch_run_id = 1007


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  ROUND 7b: REPROCESS WITH NO-OP (record already in raw)                ║
-- ║  Pattern: Request points to a record that ALREADY EXISTS in raw →       ║
-- ║           archive CTE returns nothing (no match), raw picks it up       ║
-- ║           normally → reprocess request is still marked COMPLETED        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- 7O. Insert a reprocess request for a record that is still in raw (not archived)
INSERT INTO UDL_BATCH_PROCESS.REPROCESS_REQUEST
    (ENTITY_TYPE, RECORD_CODE, TENANT_CODE, REQUESTED_BY, REASON)
VALUES
    ('NEWSLETTER', 'new-nl-round2-uuid-1234567890ab', '260cd6ce-041e-4252-abcd-cd93f9804d4e',
     'customer_support', 'Validation check — record still in raw');

-- 7P. Verify the record IS in raw
SELECT COUNT(*) AS in_raw FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
WHERE domain_payload:id::STRING = 'new-nl-round2-uuid-1234567890ab';
-- EXPECTED: >= 1

-- 7Q. Run dbt build
-- EXECUTE DBT PROJECT
--     FROM @COMMON_TENANT_DEV.DBT_UDL.DBT_STAGE
--     PROJECT_ROOT = '/newsletter_poc'
--     ARGS = 'build --target dev --vars ''{
--         "batch_run_id": 1008,
--         "data_process_end_time": "2026-03-30 00:00:00"
--     }''';

-- 7R. Reprocess request should still be completed (even though record was in raw, not archive)
SELECT COUNT(*) AS remaining FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST WHERE STATUS = 'PENDING';
-- EXPECTED: 0

SELECT COUNT(*) AS hist_count FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST_HIST
WHERE RECORD_CODE = 'new-nl-round2-uuid-1234567890ab';
-- EXPECTED: 1 (moved to history as COMPLETED)


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SUMMARY: EXPECTED PATTERN BEHAVIOR                                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
--
-- ┌─────────────────────────────────┬──────────────────────────────────────────────┐
-- │ Pattern                         │ Validated By                                 │
-- ├─────────────────────────────────┼──────────────────────────────────────────────┤
-- │ Full table load                 │ Round 1: 5 source → 4 deduped rows          │
-- │ ROW_NUMBER dedup                │ Round 1: Duplicate resolved by kafka_ts rank │
-- │ Delta dedup (new/changed)       │ Round 2: Modified NL in delta, published    │
-- │ Delta dedup (new record)        │ Round 2: New NL in delta, HIST accumulates  │
-- │ Hash-based no-op                │ Round 4: Empty delta when data unchanged    │
-- │ HIST SCD-2 (deactivate)         │ Round 2: Old version deactivated in HIST    │
-- │ HIST SCD-2 (delete event)       │ Round 3: Delete creates new active version  │
-- │ Delete flag propagation         │ Round 3: is_deleted=TRUE from event type     │
-- │ HIST-as-master publish          │ Round 5: UDL.NEWSLETTER = active HIST       │
-- │ Delta MERGE publish             │ Round 5: INTERACTION/CATEGORY via MERGE     │
-- │ History accumulation            │ Round 5: NEWSLETTER_HIST grows each build   │
-- │ Full load + archive union       │ Round 6: Raw+Archive → same result set      │
-- │ Reprocess from archive          │ Round 7: Archived record pulled via request │
-- │ Reprocess request lifecycle     │ Round 7: PENDING → COMPLETED → _HIST       │
-- │ Reprocess (already in raw)      │ Round 7b: No-op archive pull, still completes│
-- │ Multi-entity reprocess          │ Round 7: NL + INTERACTION reprocessed       │
-- │ Audit column population         │ Round 1: All dbt_* columns non-null         │
-- │ Seed reference lookup           │ Round 1: status_code, recipient_type mapped │
-- └─────────────────────────────────┴──────────────────────────────────────────────┘
