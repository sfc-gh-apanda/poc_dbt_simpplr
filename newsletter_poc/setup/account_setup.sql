-- =============================================================
-- SIMPPLR DBT PoC — Account Bootstrap Script
-- Run this ONCE in the target Snowflake account before dbt
-- =============================================================

USE ROLE SYSADMIN;

-- 1. Database
CREATE DATABASE IF NOT EXISTS SIMPPLR_DBT_DEV;
USE DATABASE SIMPPLR_DBT_DEV;

-- 2. Schemas
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_SOURCES;
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_SEEDS;
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_STAGING;
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_MARTS;
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_SNAPSHOTS;
CREATE SCHEMA IF NOT EXISTS SIMPPLR_DBT_DEFAULT;

-- =============================================================
-- 3. Source tables (mimic staging views' output structure)
-- =============================================================

CREATE OR REPLACE TABLE SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER (
    ID NUMBER(38,0),
    HEADER_ID NUMBER(38,0),
    DOMAIN_PAYLOAD VARIANT,
    TYPE VARCHAR(16777216),
    AGGREGATE_ID VARCHAR(16777216),
    METADATA VARIANT,
    KAFKA_OFFSET NUMBER(38,0),
    KAFKA_TIMESTAMP TIMESTAMP_NTZ(9),
    SOURCE_TRANSACTION_DATETIME TIMESTAMP_NTZ(9),
    HEADER VARIANT,
    CREATED_BY VARCHAR(16777216),
    CREATED_DATETIME TIMESTAMP_NTZ(9),
    UPDATED_BY VARCHAR(16777216),
    UPDATED_DATETIME TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_INTERACTION (
    ID NUMBER(38,0),
    HEADER_ID NUMBER(38,0),
    DOMAIN_PAYLOAD VARIANT,
    TYPE VARCHAR(16777216),
    AGGREGATE_ID VARCHAR(16777216),
    METADATA VARIANT,
    KAFKA_OFFSET NUMBER(38,0),
    KAFKA_TIMESTAMP TIMESTAMP_NTZ(9),
    SOURCE_TRANSACTION_DATETIME TIMESTAMP_NTZ(9),
    HEADER VARIANT,
    CREATED_BY VARCHAR(16777216),
    CREATED_DATETIME TIMESTAMP_NTZ(9),
    UPDATED_BY VARCHAR(16777216),
    UPDATED_DATETIME TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_CATEGORY (
    ID NUMBER(38,0),
    HEADER_ID NUMBER(38,0),
    DOMAIN_PAYLOAD VARIANT,
    TYPE VARCHAR(16777216),
    AGGREGATE_ID VARCHAR(16777216),
    METADATA VARIANT,
    KAFKA_OFFSET NUMBER(38,0),
    KAFKA_TIMESTAMP TIMESTAMP_NTZ(9),
    SOURCE_TRANSACTION_DATETIME TIMESTAMP_NTZ(9),
    HEADER VARIANT,
    CREATED_BY VARCHAR(16777216),
    CREATED_DATETIME TIMESTAMP_NTZ(9),
    UPDATED_BY VARCHAR(16777216),
    UPDATED_DATETIME TIMESTAMP_NTZ(9)
);

-- =============================================================
-- 4. Sample newsletter data
-- =============================================================

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER
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
    PARSE_JSON('{"aggregate_id":"85db08a2-3579-49b8-b4a4-1d80fd9021a7","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\"accountId\":\"586bea5e-0819-4b82-9462-adea5658eb47\"}"}'),
    'SYSTEM',
    '2026-03-23 13:40:22.149'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER
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
    PARSE_JSON('{"aggregate_id":"202a20ae-ba80-4d69-ad0f-46febe2e293c","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-03-20 10:35:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER
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
    PARSE_JSON('{"aggregate_id":"del-nl-001-uuid-here-1234567890ab","eventType":"NEWSLETTER_DELETED","tenant_info":"{\"accountId\":\"586bea5e-0819-4b82-9462-adea5658eb47\"}"}'),
    'SYSTEM',
    '2026-03-18 14:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER
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
    PARSE_JSON('{"aggregate_id":"sch-nl-001-uuid-here-1234567890ab","eventType":"NEWSLETTER_MODIFIED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-03-22 16:35:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

-- Duplicate record for first newsletter (older version, same code+tenant) to test dedup
INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER
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
    PARSE_JSON('{"aggregate_id":"85db08a2-3579-49b8-b4a4-1d80fd9021a7","eventType":"NEWSLETTER_CREATED","tenant_info":"{\"accountId\":\"586bea5e-0819-4b82-9462-adea5658eb47\"}"}'),
    'SYSTEM',
    '2026-03-23 13:40:22.149'::TIMESTAMP_NTZ,
    NULL, NULL;


-- =============================================================
-- 5. Sample interaction data
-- =============================================================

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_INTERACTION
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
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-03-20 11:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_INTERACTION
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
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-03-20 12:35:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_INTERACTION
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
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\"accountId\":\"586bea5e-0819-4b82-9462-adea5658eb47\"}"}'),
    'SYSTEM',
    '2026-03-23 14:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_INTERACTION
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
    PARSE_JSON('{"eventType":"NEWSLETTER_INTERACTION_CREATED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-03-20 15:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;


-- =============================================================
-- 6. Sample category data
-- =============================================================

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_CATEGORY
SELECT
    30001, 30000,
    PARSE_JSON('{"id": "cat-001-uuid-here-1234567890ab", "name": "Company News", "created_at": "2026-01-15T10:00:00.000Z"}'),
    'NEWSLETTER_CATEGORY_CREATED',
    'cat-001-uuid-here-1234567890ab',
    NULL,
    60001,
    '2026-01-15 10:00:01.000'::TIMESTAMP_NTZ,
    '2026-01-15 10:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_CATEGORY_CREATED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-01-15 10:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_CATEGORY
SELECT
    30002, 30001,
    PARSE_JSON('{"id": "cat-002-uuid-here-1234567890ab", "name": "HR Updates", "created_at": "2026-02-01T08:00:00.000Z"}'),
    'NEWSLETTER_CATEGORY_CREATED',
    'cat-002-uuid-here-1234567890ab',
    NULL,
    60002,
    '2026-02-01 08:00:01.000'::TIMESTAMP_NTZ,
    '2026-02-01 08:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_CATEGORY_CREATED","tenant_info":"{\"accountId\":\"586bea5e-0819-4b82-9462-adea5658eb47\"}"}'),
    'SYSTEM',
    '2026-02-01 08:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;

INSERT INTO SIMPPLR_DBT_SOURCES.VW_ENL_NEWSLETTER_CATEGORY
SELECT
    30003, 30002,
    PARSE_JSON('{"id": "cat-003-uuid-here-1234567890ab", "name": "Product Launches", "created_at": "2026-03-01T12:00:00.000Z"}'),
    'NEWSLETTER_CATEGORY_CREATED',
    'cat-003-uuid-here-1234567890ab',
    NULL,
    60003,
    '2026-03-01 12:00:01.000'::TIMESTAMP_NTZ,
    '2026-03-01 12:00:00.000'::TIMESTAMP_NTZ,
    PARSE_JSON('{"eventType":"NEWSLETTER_CATEGORY_CREATED","tenant_info":"{\"accountId\":\"260cd6ce-041e-4252-abcd-cd93f9804d4e\"}"}'),
    'SYSTEM',
    '2026-03-01 12:05:00.000'::TIMESTAMP_NTZ,
    NULL, NULL;
