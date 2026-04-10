-- ═══════════════════════════════════════════════════════════════════════════════
-- SIMPPLR DBT PoC — Account Bootstrap Script
-- Run this ONCE in the target Snowflake account BEFORE all other setup scripts.
--
-- Creates:
--   1. Database and schemas
--   2. Source tables (3 raw views)
--   3. Archive tables (3, same structure as raw)
--   4. UDL target tables (NEWSLETTER, NEWSLETTER_INTERACTION, NEWSLETTER_CATEGORY)
--   5. UDL.NEWSLETTER_HIST (master SCD-2 accumulator with active_flag tracking)
--   6. REPROCESS_REQUEST + _HIST (customer-driven reprocessing queue)
--   7. Sample data (newsletters, interactions, categories)
--   8. External access integration for dbt packages (dbt_utils, dbt_expectations)
--
-- Prerequisites:
--   - ACCOUNTADMIN role (or equivalent with CREATE DATABASE / INTEGRATION)
--   - Warehouse COMPUTE_WH (or adjust USE WAREHOUSE below)
--
-- Execution order:
--   1. account_bootstrap.sql   ← THIS FILE
--   2. audit_setup.sql
--   3. publish_archive_setup.sql
--   4. retry_setup.sql
--   5. test_logging_setup.sql
--   6. monitoring_queries.sql   (optional)
--   7. dashboard_queries.sql    (optional)
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. DATABASE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS COMMON_TENANT_DEV;
USE DATABASE COMMON_TENANT_DEV;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. SCHEMAS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS SHARED_SERVICES_STAGING
    COMMENT = 'Raw source views and archive tables (Kafka-ingested newsletter events)';

CREATE SCHEMA IF NOT EXISTS UDL
    COMMENT = 'User-facing target tables — NEWSLETTER derived from NEWSLETTER_HIST, INTERACTION/CATEGORY via MERGE';

CREATE SCHEMA IF NOT EXISTS DBT_UDL
    COMMENT = 'dbt delta work tables (wrk_*, seeds, pipeline_complete)';

CREATE SCHEMA IF NOT EXISTS UDL_BATCH_PROCESS
    COMMENT = 'Stored procedures for publish, archive, and retry operations';

CREATE SCHEMA IF NOT EXISTS DBT_EXECUTION_RUN_STATS
    COMMENT = 'dbt processing tracking, audit, and observability';


-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. SOURCE TABLES — Raw views (mimic Kafka staging output)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER (
    ID                          NUMBER(38,0),
    HEADER_ID                   NUMBER(38,0),
    DOMAIN_PAYLOAD              VARIANT,
    TYPE                        VARCHAR(16777216),
    AGGREGATE_ID                VARCHAR(16777216),
    METADATA                    VARIANT,
    KAFKA_OFFSET                NUMBER(38,0),
    KAFKA_TIMESTAMP             TIMESTAMP_NTZ(9),
    SOURCE_TRANSACTION_DATETIME TIMESTAMP_NTZ(9),
    HEADER                      VARIANT,
    CREATED_BY                  VARCHAR(16777216),
    CREATED_DATETIME            TIMESTAMP_NTZ(9),
    UPDATED_BY                  VARCHAR(16777216),
    UPDATED_DATETIME            TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION (
    ID                          NUMBER(38,0),
    HEADER_ID                   NUMBER(38,0),
    DOMAIN_PAYLOAD              VARIANT,
    TYPE                        VARCHAR(16777216),
    AGGREGATE_ID                VARCHAR(16777216),
    METADATA                    VARIANT,
    KAFKA_OFFSET                NUMBER(38,0),
    KAFKA_TIMESTAMP             TIMESTAMP_NTZ(9),
    SOURCE_TRANSACTION_DATETIME TIMESTAMP_NTZ(9),
    HEADER                      VARIANT,
    CREATED_BY                  VARCHAR(16777216),
    CREATED_DATETIME            TIMESTAMP_NTZ(9),
    UPDATED_BY                  VARCHAR(16777216),
    UPDATED_DATETIME            TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY (
    ID                          NUMBER(38,0),
    HEADER_ID                   NUMBER(38,0),
    DOMAIN_PAYLOAD              VARIANT,
    TYPE                        VARCHAR(16777216),
    AGGREGATE_ID                VARCHAR(16777216),
    METADATA                    VARIANT,
    KAFKA_OFFSET                NUMBER(38,0),
    KAFKA_TIMESTAMP             TIMESTAMP_NTZ(9),
    SOURCE_TRANSACTION_DATETIME TIMESTAMP_NTZ(9),
    HEADER                      VARIANT,
    CREATED_BY                  VARCHAR(16777216),
    CREATED_DATETIME            TIMESTAMP_NTZ(9),
    UPDATED_BY                  VARCHAR(16777216),
    UPDATED_DATETIME            TIMESTAMP_NTZ(9)
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. ARCHIVE TABLES — Same structure as raw, used by staging UNION ALL (full-load)
--    and archive procedure (post-publish)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS SHARED_SERVICES_STAGING.ENL_NEWSLETTER_ARCHIVE
    LIKE SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER;

CREATE TABLE IF NOT EXISTS SHARED_SERVICES_STAGING.ENL_NEWSLETTER_INTERACTION_ARCHIVE
    LIKE SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION;

CREATE TABLE IF NOT EXISTS SHARED_SERVICES_STAGING.ENL_NEWSLETTER_CATEGORY_ARCHIVE
    LIKE SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 5. UDL TARGET TABLES — Must pre-exist for the HIST-as-master publish procedure.
--    NEWSLETTER: derived from NEWSLETTER_HIST (active_flag = TRUE).
--    INTERACTION / CATEGORY: populated via delta MERGE from wrk_* tables.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS UDL.NEWSLETTER (
    ID                              NUMBER(38,0) NOT NULL,
    DATA_SOURCE_CODE                VARCHAR(16),
    TENANT_CODE                     VARCHAR(255) NOT NULL,
    STAGING_ID                      NUMBER(38,0),
    CODE                            VARCHAR(255) NOT NULL,
    NAME                            VARCHAR(16777216),
    SUBJECT                         VARCHAR(16777216),
    SENDER_ADDRESS                  VARCHAR(16777216),
    SEND_AS_EMAIL                   BOOLEAN,
    SEND_AS_SMS                     BOOLEAN,
    SEND_AS_MS_TEAMS_MESSAGE        BOOLEAN,
    SEND_AS_SLACK_MESSAGE           BOOLEAN,
    SEND_AS_INTRANET                BOOLEAN,
    SCHEDULED_AT                    TIMESTAMP_NTZ,
    SENT_AT                         TIMESTAMP_NTZ,
    NEWSLETTER_CREATED_BY_CODE      VARCHAR(255),
    NEWSLETTER_UPDATED_BY_CODE      VARCHAR(255),
    NEWSLETTER_CREATED_DATETIME     TIMESTAMP_NTZ,
    NEWSLETTER_UPDATED_DATETIME     TIMESTAMP_NTZ,
    STATUS_CODE                     VARCHAR(255),
    CATEGORY_CODE                   VARCHAR(255),
    TEMPLATE_CODE                   VARCHAR(255),
    THEME_CODE                      VARCHAR(255),
    IS_ARCHIVED                     BOOLEAN,
    SEND_AS_TIMEZONE_AWARE_SCHEDULE BOOLEAN,
    REPLY_TO_EMAIL_ADDRESS          VARCHAR(16777216),
    RECIPIENT_INFO                  VARIANT,
    RECIPIENT_TYPE_CODE             VARCHAR(255),
    IS_DELETED                      BOOLEAN,
    DELETED_NOTE                    VARCHAR(16777216),
    DELETED_DATETIME                TIMESTAMP_NTZ,
    ACTIVE_FLAG                     BOOLEAN NOT NULL,
    ACTIVE_DATE                     TIMESTAMP_NTZ,
    INACTIVE_DATE                   TIMESTAMP_NTZ,
    CREATED_BY                      VARCHAR(255),
    CREATED_DATETIME                TIMESTAMP_NTZ,
    UPDATED_BY                      VARCHAR(255),
    UPDATED_DATETIME                TIMESTAMP_NTZ,
    HASH_VALUE                      VARCHAR(32) NOT NULL,
    RECIPIENT_NAME                  VARCHAR(16777216),
    ACTUAL_DELIVERY_SYSTEM_TYPE     VARCHAR(16777216),
    BATCH_RUN_ID                    NUMBER(38,0),
    DBT_LOADED_AT                   TIMESTAMP_NTZ,
    DBT_RUN_ID                      VARCHAR(50),
    DBT_BATCH_ID                    VARCHAR(32),
    DBT_SOURCE_MODEL                VARCHAR(100),
    DBT_ENVIRONMENT                 VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS UDL.NEWSLETTER_INTERACTION (
    ID                              NUMBER(38,0) NOT NULL,
    DATA_SOURCE_CODE                VARCHAR(16),
    TENANT_CODE                     VARCHAR(255) NOT NULL,
    STAGING_ID                      NUMBER(38,0),
    CODE                            VARCHAR(255) NOT NULL,
    NEWSLETTER_CODE                 VARCHAR(255),
    RECIPIENT_CODE                  VARCHAR(16777216),
    INTERACTION_TYPE_CODE           VARCHAR(255),
    DELIVERY_SYSTEM_TYPE_CODE       VARCHAR(255),
    DEVICE_TYPE_CODE                VARCHAR(16),
    TOTAL_TIME_SPENT_ON_INTRANET_HUB NUMBER(38,0),
    RECIPIENT_CURRENT_CITY          VARCHAR(16777216),
    RECIPIENT_CURRENT_COUNTRY       VARCHAR(16777216),
    RECIPIENT_CURRENT_DEPARTMENT    VARCHAR(16777216),
    RECIPIENT_CURRENT_LOCATION      VARCHAR(16777216),
    RECIPIENT_CURRENT_EMAIL         VARCHAR(16777216),
    INTERACTION_DATETIME            TIMESTAMP_NTZ,
    RECIPIENT_TYPE_CODE             VARCHAR(255),
    BLOCK_CODE                      VARCHAR(255),
    BLOCK_TYPE_CODE                 VARCHAR(255),
    CLICK_TYPE_CODE                 VARCHAR(255),
    IP_ADDRESS                      VARCHAR(255),
    LINK_CLICKED_PAGE_TITLE         VARCHAR(16777216),
    LINK_CLICKED_URL                VARCHAR(16777216),
    SESSION_ID                      VARCHAR(255),
    RECIPIENT_CURRENT_TIMEZONE      VARCHAR(255),
    USER_AGENT                      VARCHAR(16777216),
    ACTIVE_FLAG                     BOOLEAN NOT NULL,
    ACTIVE_DATE                     TIMESTAMP_NTZ,
    CREATED_BY                      VARCHAR(255),
    CREATED_DATETIME                TIMESTAMP_NTZ,
    UPDATED_BY                      VARCHAR(255),
    UPDATED_DATETIME                TIMESTAMP_NTZ,
    HASH_VALUE                      VARCHAR(32) NOT NULL,
    BATCH_RUN_ID                    NUMBER(38,0),
    DBT_LOADED_AT                   TIMESTAMP_NTZ,
    DBT_RUN_ID                      VARCHAR(50),
    DBT_BATCH_ID                    VARCHAR(32),
    DBT_SOURCE_MODEL                VARCHAR(100),
    DBT_ENVIRONMENT                 VARCHAR(20),
    PUBLISHED_BY_RUN_ID             VARCHAR(100),
    PUBLISHED_AT                    TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS UDL.NEWSLETTER_CATEGORY (
    ID                              NUMBER(38,0) NOT NULL,
    DATA_SOURCE_CODE                VARCHAR(16),
    TENANT_CODE                     VARCHAR(255) NOT NULL,
    STAGING_ID                      NUMBER(38,0),
    CODE                            VARCHAR(255) NOT NULL,
    NAME                            VARCHAR(16777216),
    CATEGORY_CREATED_DATETIME       TIMESTAMP_NTZ,
    ACTIVE_FLAG                     BOOLEAN NOT NULL,
    ACTIVE_DATE                     TIMESTAMP_NTZ,
    INACTIVE_DATE                   TIMESTAMP_NTZ,
    CREATED_BY                      VARCHAR(255),
    CREATED_DATETIME                TIMESTAMP_NTZ,
    UPDATED_BY                      VARCHAR(255),
    UPDATED_DATETIME                TIMESTAMP_NTZ,
    HASH_VALUE                      VARCHAR(32) NOT NULL,
    BATCH_RUN_ID                    NUMBER(38,0),
    DBT_LOADED_AT                   TIMESTAMP_NTZ,
    DBT_RUN_ID                      VARCHAR(50),
    DBT_BATCH_ID                    VARCHAR(32),
    DBT_SOURCE_MODEL                VARCHAR(100),
    DBT_ENVIRONMENT                 VARCHAR(20),
    PUBLISHED_BY_RUN_ID             VARCHAR(100),
    PUBLISHED_AT                    TIMESTAMP_NTZ
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 5b. CLUSTERING — Accelerates hash-dedup JOINs, MERGE publish, and HIST queries.
--     Critical for multi-billion row tables (especially NEWSLETTER_INTERACTION).
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE UDL.NEWSLETTER CLUSTER BY (TENANT_CODE, CODE);
ALTER TABLE UDL.NEWSLETTER_INTERACTION CLUSTER BY (TENANT_CODE, CODE);
ALTER TABLE UDL.NEWSLETTER_CATEGORY CLUSTER BY (TENANT_CODE, CODE);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 6. UDL.NEWSLETTER_HIST — Master SCD-2 accumulator (HIST-as-master pattern)
--    Contains ALL versions with active_flag tracking:
--      active_flag=TRUE  → current active record (appears in UDL.NEWSLETTER)
--      active_flag=FALSE → superseded historical version
--    Column order matches wrk_newsletter contract + publish tracking columns.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS UDL.NEWSLETTER_HIST (
    id                              NUMBER(38,0),
    data_source_code                VARCHAR(16),
    tenant_code                     VARCHAR(255),
    staging_id                      NUMBER(38,0),
    code                            VARCHAR(255),
    name                            VARCHAR(16777216),
    subject                         VARCHAR(16777216),
    sender_address                  VARCHAR(16777216),
    send_as_email                   BOOLEAN,
    send_as_sms                     BOOLEAN,
    send_as_ms_teams_message        BOOLEAN,
    send_as_slack_message           BOOLEAN,
    send_as_intranet                BOOLEAN,
    scheduled_at                    TIMESTAMP_NTZ,
    sent_at                         TIMESTAMP_NTZ,
    newsletter_created_by_code      VARCHAR(255),
    newsletter_updated_by_code      VARCHAR(255),
    newsletter_created_datetime     TIMESTAMP_NTZ,
    newsletter_updated_datetime     TIMESTAMP_NTZ,
    status_code                     VARCHAR(255),
    category_code                   VARCHAR(255),
    template_code                   VARCHAR(255),
    theme_code                      VARCHAR(255),
    is_archived                     BOOLEAN,
    send_as_timezone_aware_schedule BOOLEAN,
    reply_to_email_address          VARCHAR(16777216),
    recipient_info                  VARIANT,
    recipient_type_code             VARCHAR(255),
    is_deleted                      BOOLEAN,
    deleted_note                    VARCHAR(16777216),
    deleted_datetime                TIMESTAMP_NTZ,
    active_flag                     BOOLEAN,
    active_date                     TIMESTAMP_NTZ,
    inactive_date                   TIMESTAMP_NTZ,
    created_by                      VARCHAR(255),
    created_datetime                TIMESTAMP_NTZ,
    updated_by                      VARCHAR(255),
    updated_datetime                TIMESTAMP_NTZ,
    hash_value                      VARCHAR(32),
    recipient_name                  VARCHAR(16777216),
    actual_delivery_system_type     VARCHAR(16777216),
    -- dbt audit columns (matches wrk_newsletter contract)
    batch_run_id                    NUMBER(38,0),
    dbt_loaded_at                   TIMESTAMP_NTZ,
    dbt_run_id                      VARCHAR(50),
    dbt_batch_id                    VARCHAR(32),
    dbt_source_model                VARCHAR(100),
    dbt_environment                 VARCHAR(20),
    -- publish tracking columns (appended by PRC_DBT_PUBLISH_TO_TARGET)
    published_by_run_id             VARCHAR(100),
    published_at                    TIMESTAMP_NTZ
);

ALTER TABLE UDL.NEWSLETTER_HIST CLUSTER BY (TENANT_CODE, CODE, ACTIVE_FLAG);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 6b. REPROCESS_REQUEST — Customer-driven reprocessing queue
--     Records queued here are fetched from archive tables and merged into the
--     staging pipeline alongside raw view data.
--     Status lifecycle: PENDING → COMPLETED (after publish succeeds)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS UDL_BATCH_PROCESS.REPROCESS_REQUEST (
    REQUEST_ID              NUMBER AUTOINCREMENT,
    ENTITY_TYPE             VARCHAR(50)    NOT NULL,
    RECORD_CODE             VARCHAR(255)   NOT NULL,
    TENANT_CODE             VARCHAR(255)   NOT NULL,
    REQUESTED_BY            VARCHAR(100),
    REQUESTED_AT            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    REASON                  VARCHAR(500),
    STATUS                  VARCHAR(20)    DEFAULT 'PENDING',
    PROCESSED_BY_RUN_ID     VARCHAR(50),
    PROCESSED_AT            TIMESTAMP_NTZ,
    BATCH_RUN_ID            INTEGER,
    CONSTRAINT uq_reprocess UNIQUE (ENTITY_TYPE, RECORD_CODE, TENANT_CODE, STATUS)
);

CREATE TABLE IF NOT EXISTS UDL_BATCH_PROCESS.REPROCESS_REQUEST_HIST (
    REQUEST_ID              NUMBER,
    ENTITY_TYPE             VARCHAR(50),
    RECORD_CODE             VARCHAR(255),
    TENANT_CODE             VARCHAR(255),
    REQUESTED_BY            VARCHAR(100),
    REQUESTED_AT            TIMESTAMP_NTZ,
    REASON                  VARCHAR(500),
    STATUS                  VARCHAR(20),
    PROCESSED_BY_RUN_ID     VARCHAR(50),
    PROCESSED_AT            TIMESTAMP_NTZ,
    BATCH_RUN_ID            INTEGER
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 7. SAMPLE DATA — Newsletter events
--    5 records: 4 unique newsletters + 1 duplicate (older version, same
--    code+tenant) to test hash-based deduplication.
-- ═══════════════════════════════════════════════════════════════════════════════

-- Newsletter 1: Draft (modified version)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12239, 12233,
    PARSE_JSON('{
        "id": "85db08a2-3579-49b8-b4a4-1d80fd9021a7",
        "name": "Copy of test",
        "subject": "test newsletter subject",
        "status": "draft",
        "channels": {
            "email": {"type": "email", "sender_address": "noreply@dev.simpplr.xyz", "domain": "dev.simpplr.xyz"},
            "intranet": {"type": "intranet"}
        },
        "recipients": [{"count": 4, "id": "32d2063e-7230-4f3d-b3a7-262ae8ed4b8d", "name": "Newsletter india", "type": "audience"}],
        "template": {"id": "7fb09d8d-953f-4c1a-882d-97e152d19808", "name": "Blank template"},
        "theme": {"id": "cfe73014-ad57-4eaa-9aa0-1bceec68ac34", "name": "default theme"},
        "creator_id": "b7ec2600-5143-4c4a-804a-030b445510a6",
        "modifier_id": "b7ec2600-5143-4c4a-804a-030b445510a6",
        "created_at": "2026-03-23T13:34:57.928Z",
        "modified_at": "2026-03-23T13:35:08.572Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": false,
        "reply_to_address": "noreply@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_MODIFIED',
    '85db08a2-3579-49b8-b4a4-1d80fd9021a7',
    NULL,
    34277,
    '2026-03-23 13:35:08.935'::TIMESTAMP_NTZ,
    '2026-03-23 13:35:08.572'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"85db08a2-3579-49b8-b4a4-1d80fd9021a7","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\\\"accountId\\\":\\\"586bea5e-0819-4b82-9462-adea5658eb47\\\"}"}'),
    'SYSTEM',
    '2026-03-23 13:40:22.149'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Newsletter 2: Sent, multi-channel (email + SMS + Teams), with category
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12240, 12234,
    PARSE_JSON('{
        "id": "202a20ae-ba80-4d69-ad0f-46febe2e293c",
        "name": "Weekly Update",
        "subject": "Your weekly digest",
        "status": "sent",
        "channels": {
            "email": {"type": "email", "sender_address": "updates@dev.simpplr.xyz", "domain": "dev.simpplr.xyz"},
            "sms": {"type": "sms"},
            "msTeams": {"type": "msTeams"}
        },
        "recipients": [
            {"count": 150, "id": "aaa11111-1111-1111-1111-111111111111", "name": "All Employees", "type": "audience"},
            {"count": 25, "id": "bbb22222-2222-2222-2222-222222222222", "name": "Engineering", "type": "audience"}
        ],
        "category": {"id": "cat-001-uuid-here-1234567890ab"},
        "template": {"id": "tmpl-002-uuid-here-1234567890ab", "name": "Weekly Digest"},
        "theme": {"id": "thm-002-uuid-here-12345678901234", "name": "corporate theme"},
        "creator_id": "usr-creator-0001-uuid-12345678",
        "modifier_id": "usr-modifier-0001-uuid-1234567",
        "created_at": "2026-03-20T09:00:00.000Z",
        "modified_at": "2026-03-20T10:30:00.000Z",
        "sent_at": "2026-03-20T10:30:00.000Z",
        "send_at": "2026-03-20T10:00:00.000Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": true,
        "reply_to_address": "updates@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_MODIFIED',
    '202a20ae-ba80-4d69-ad0f-46febe2e293c',
    NULL,
    34278,
    '2026-03-20 10:30:01.000'::TIMESTAMP_NTZ,
    '2026-03-20 10:30:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"202a20ae-ba80-4d69-ad0f-46febe2e293c","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-20 10:35:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Newsletter 3: Deleted newsletter (is_archived=true, NEWSLETTER_DELETED event)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12241, 12235,
    PARSE_JSON('{
        "id": "del-nl-001-uuid-here-1234567890ab",
        "name": "Old Announcement",
        "subject": "Deprecated announcement",
        "status": "sent",
        "channels": {"email": {"type": "email", "sender_address": "news@dev.simpplr.xyz"}},
        "recipients": [{"count": 50, "id": "grp-001-uuid-here-1234567890ab", "name": "HR Team", "type": "audience", "include_followers": true}],
        "template": {"id": "tmpl-003-uuid-here-1234567890ab"},
        "theme": {"id": "thm-003-uuid-here-12345678901234"},
        "creator_id": "usr-creator-0002-uuid-12345678",
        "modifier_id": "usr-modifier-0002-uuid-1234567",
        "created_at": "2026-03-15T08:00:00.000Z",
        "modified_at": "2026-03-18T14:00:00.000Z",
        "sent_at": "2026-03-16T09:00:00.000Z",
        "is_archived": true,
        "send_as_timezone_aware_schedule": false,
        "reply_to_address": "hr@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_DELETED',
    'del-nl-001-uuid-here-1234567890ab',
    NULL,
    34279,
    '2026-03-18 14:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-18 14:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"del-nl-001-uuid-here-1234567890ab","eventType":"NEWSLETTER_DELETED","tenant_info":"{\\\"accountId\\\":\\\"586bea5e-0819-4b82-9462-adea5658eb47\\\"}"}'),
    'SYSTEM',
    '2026-03-18 14:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Newsletter 4: Scheduled, multi-channel (email + Slack)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12242, 12236,
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
        "modified_at": "2026-03-22T16:30:00.000Z",
        "send_at": "2026-03-25T09:00:00.000Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": true,
        "reply_to_address": "monthly@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_MODIFIED',
    'sch-nl-001-uuid-here-1234567890ab',
    NULL,
    34280,
    '2026-03-22 16:30:01.000'::TIMESTAMP_NTZ,
    '2026-03-22 16:30:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"sch-nl-001-uuid-here-1234567890ab","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-22 16:35:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Newsletter 1 (DUPLICATE): Older version of newsletter 1 (same code+tenant, earlier timestamp)
-- Tests hash-based deduplication — this record should be superseded by the modified version above
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
SELECT
    12238, 12232,
    PARSE_JSON('{
        "id": "85db08a2-3579-49b8-b4a4-1d80fd9021a7",
        "name": "test",
        "subject": "test newsletter subject",
        "status": "draft",
        "channels": {"email": {"type": "email", "sender_address": "noreply@dev.simpplr.xyz"}},
        "recipients": [{"count": 4, "id": "32d2063e-7230-4f3d-b3a7-262ae8ed4b8d", "name": "Newsletter india", "type": "audience"}],
        "template": {"id": "7fb09d8d-953f-4c1a-882d-97e152d19808"},
        "theme": {"id": "cfe73014-ad57-4eaa-9aa0-1bceec68ac34"},
        "creator_id": "b7ec2600-5143-4c4a-804a-030b445510a6",
        "modifier_id": "b7ec2600-5143-4c4a-804a-030b445510a6",
        "created_at": "2026-03-23T13:34:57.928Z",
        "modified_at": "2026-03-23T13:34:57.928Z",
        "is_archived": false,
        "send_as_timezone_aware_schedule": false,
        "reply_to_address": "noreply@dev.simpplr.xyz"
    }'),
    'NEWSLETTER_CREATED',
    '85db08a2-3579-49b8-b4a4-1d80fd9021a7',
    NULL,
    34275,
    '2026-03-23 13:34:58.694'::TIMESTAMP_NTZ,
    '2026-03-23 13:34:57.928'::TIMESTAMP_NTZ,
    PARSE_JSON('{"aggregate_id":"85db08a2-3579-49b8-b4a4-1d80fd9021a7","eventType":"NEWSLETTER_CREATED","tenant_info":"{\\\"accountId\\\":\\\"586bea5e-0819-4b82-9462-adea5658eb47\\\"}"}'),
    'SYSTEM',
    '2026-03-23 13:40:22.149'::TIMESTAMP_NTZ,
    NULL, NULL;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 8. SAMPLE DATA — Newsletter interaction events
--    4 records covering different interaction types and delivery channels.
-- ═══════════════════════════════════════════════════════════════════════════════

-- Interaction 1: Email open
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
SELECT
    20001, 20000,
    PARSE_JSON('{
        "id": "int-001-uuid-here-1234567890ab",
        "newsletter": {"id": "202a20ae-ba80-4d69-ad0f-46febe2e293c", "recipients": [{"type": "audience", "include_followers": false}]},
        "user": {"people_id": "usr-recip-0001-uuid-12345678", "city": "San Francisco", "country": "US", "department": "Engineering", "location": "HQ", "email": "john@example.com", "timezone_iso": "America/Los_Angeles"},
        "interaction": "opened",
        "delivery_system_type": "email",
        "timestamp": "2026-03-20T11:00:00.000Z",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "ip_address": "192.168.1.1",
        "session_id": "sess-001"
    }'),
    'NEWSLETTER_INTERACTION_CREATED',
    'int-001-uuid-here-1234567890ab',
    NULL,
    50001,
    '2026-03-20 11:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-20 11:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-20 11:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Interaction 2: Email link click (with block and click metadata)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
SELECT
    20002, 20001,
    PARSE_JSON('{
        "id": "int-002-uuid-here-1234567890ab",
        "newsletter": {"id": "202a20ae-ba80-4d69-ad0f-46febe2e293c", "recipients": [{"type": "audience"}]},
        "user": {"people_id": "usr-recip-0002-uuid-12345678", "city": "London", "country": "UK", "department": "Marketing", "location": "London Office", "email": "jane@example.com", "timezone_iso": "Europe/London"},
        "interaction": "clicked",
        "delivery_system_type": "email",
        "click_type": "link",
        "block_type": "text",
        "block_id": "blk-001",
        "title": "Read more",
        "url": "https://example.com/article",
        "timestamp": "2026-03-20T12:30:00.000Z",
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0)",
        "ip_address": "10.0.0.1",
        "session_id": "sess-002"
    }'),
    'NEWSLETTER_INTERACTION_CREATED',
    'int-002-uuid-here-1234567890ab',
    NULL,
    50002,
    '2026-03-20 12:30:01.000'::TIMESTAMP_NTZ,
    '2026-03-20 12:30:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-20 12:35:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Interaction 3: Intranet send (sparse user data, tests COALESCE defaults)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
SELECT
    20003, 20002,
    PARSE_JSON('{
        "id": "int-003-uuid-here-1234567890ab",
        "newsletter": {"id": "85db08a2-3579-49b8-b4a4-1d80fd9021a7", "recipients": [{"type": "audience"}]},
        "user": {"people_id": "usr-recip-0003-uuid-12345678", "city": "", "country": "", "department": "", "location": "", "email": "", "timezone_iso": ""},
        "interaction": "sent",
        "delivery_system_type": "intranet",
        "seconds_on_app": 45,
        "timestamp": "2026-03-23T14:00:00.000Z",
        "user_agent": "Simpplr native mobile app iOS/3.5",
        "ip_address": "172.16.0.1",
        "session_id": "sess-003"
    }'),
    'NEWSLETTER_INTERACTION_CREATED',
    'int-003-uuid-here-1234567890ab',
    NULL,
    50003,
    '2026-03-23 14:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-23 14:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\\\"accountId\\\":\\\"586bea5e-0819-4b82-9462-adea5658eb47\\\"}"}'),
    'SYSTEM',
    '2026-03-23 14:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Interaction 4: MS Teams open (same user as interaction 1, different channel)
INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
SELECT
    20004, 20003,
    PARSE_JSON('{
        "id": "int-004-uuid-here-1234567890ab",
        "newsletter": {"id": "202a20ae-ba80-4d69-ad0f-46febe2e293c", "recipients": [{"type": "audience"}]},
        "user": {"people_id": "usr-recip-0001-uuid-12345678", "city": "San Francisco", "country": "US", "department": "Engineering", "location": "HQ", "email": "john@example.com", "timezone_iso": "America/Los_Angeles"},
        "interaction": "opened",
        "delivery_system_type": "msTeams",
        "timestamp": "2026-03-20T15:00:00.000Z",
        "user_agent": "Microsoft Teams Desktop App/1.6",
        "ip_address": "192.168.1.1",
        "session_id": "sess-004"
    }'),
    'NEWSLETTER_INTERACTION_CREATED',
    'int-004-uuid-here-1234567890ab',
    NULL,
    50004,
    '2026-03-20 15:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-20 15:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-20 15:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 9. SAMPLE DATA — Newsletter category events
--    3 records covering different tenants.
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
SELECT
    30001, 30000,
    PARSE_JSON('{"id": "cat-001-uuid-here-1234567890ab", "name": "Company News", "created_at": "2026-01-15T10:00:00.000Z"}'),
    'NEWSLETTER_CATEGORY_CREATED',
    'cat-001-uuid-here-1234567890ab',
    NULL,
    60001,
    '2026-01-15 10:00:01.000'::TIMESTAMP_NTZ,
    '2026-01-15 10:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_CATEGORY_CREATED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-01-15 10:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
SELECT
    30002, 30001,
    PARSE_JSON('{"id": "cat-002-uuid-here-1234567890ab", "name": "HR Updates", "created_at": "2026-02-01T08:00:00.000Z"}'),
    'NEWSLETTER_CATEGORY_CREATED',
    'cat-002-uuid-here-1234567890ab',
    NULL,
    60002,
    '2026-02-01 08:00:01.000'::TIMESTAMP_NTZ,
    '2026-02-01 08:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_CATEGORY_CREATED","tenant_info":"{\\\"accountId\\\":\\\"586bea5e-0819-4b82-9462-adea5658eb47\\\"}"}'),
    'SYSTEM',
    '2026-02-01 08:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
SELECT
    30003, 30002,
    PARSE_JSON('{"id": "cat-003-uuid-here-1234567890ab", "name": "Product Launches", "created_at": "2026-03-01T12:00:00.000Z"}'),
    'NEWSLETTER_CATEGORY_CREATED',
    'cat-003-uuid-here-1234567890ab',
    NULL,
    60003,
    '2026-03-01 12:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-01 12:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_CATEGORY_CREATED","tenant_info":"{\\\"accountId\\\":\\\"260cd6ce-041e-4252-abcd-cd93f9804d4e\\\"}"}'),
    'SYSTEM',
    '2026-03-01 12:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 10. EXTERNAL ACCESS INTEGRATION — Required for dbt deps (dbt_utils, dbt_expectations)
--     Skip this section if your account already has an integration for dbt Hub / GitHub.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE NETWORK RULE IF NOT EXISTS COMMON_TENANT_DEV.PUBLIC.DBT_HUB_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'hub.getdbt.com',
        'codeload.github.com',
        'github.com',
        'objects.githubusercontent.com'
    );

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS DBT_HUB_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (COMMON_TENANT_DEV.PUBLIC.DBT_HUB_NETWORK_RULE)
    ENABLED = TRUE;


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES — Run after bootstrap to confirm object creation
-- ═══════════════════════════════════════════════════════════════════════════════

-- Check schemas
SELECT SCHEMA_NAME FROM COMMON_TENANT_DEV.INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = 'COMMON_TENANT_DEV'
ORDER BY SCHEMA_NAME;

-- Check source tables
SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT
FROM COMMON_TENANT_DEV.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SHARED_SERVICES_STAGING'
ORDER BY TABLE_NAME;

-- Check UDL tables (NEWSLETTER, NEWSLETTER_INTERACTION, NEWSLETTER_CATEGORY, NEWSLETTER_HIST — all empty until first publish)
SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT
FROM COMMON_TENANT_DEV.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'UDL'
ORDER BY TABLE_NAME;

-- Check sample data counts
SELECT 'NEWSLETTER' AS entity, COUNT(*) AS row_count FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
UNION ALL
SELECT 'INTERACTION', COUNT(*) FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
UNION ALL
SELECT 'CATEGORY', COUNT(*) FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY;
