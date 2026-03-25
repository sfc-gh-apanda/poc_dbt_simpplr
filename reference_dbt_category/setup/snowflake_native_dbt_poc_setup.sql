-- =====================================================
-- SNOWFLAKE NATIVE DBT - POC SETUP (NO GIT)
-- Deploy dbt project from Stage
-- =====================================================

USE ROLE R_DEPLOYMENT_ADMIN_DEV;
USE DATABASE COMMON_TENANT_DEV;
USE WAREHOUSE WH_ETL_DEV;

-- =====================================================
-- STEP 1: Create Stage for dbt Project
-- =====================================================
CREATE OR REPLACE STAGE DBT_CONTROL.DBT_PROJECT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for dbt project files';

-- =====================================================
-- STEP 2: Upload dbt project files to stage
-- Run these commands from SnowSQL or Snowsight
-- =====================================================

-- Upload from workspace to stage
-- Option A: Copy from workspace
COPY FILES INTO @DBT_CONTROL.DBT_PROJECT_STAGE/dbt_dev/
FROM 'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/dbt_dev/'
PATTERN = '.*';

-- Verify files uploaded
LIST @DBT_CONTROL.DBT_PROJECT_STAGE/dbt_dev/;

-- =====================================================
-- STEP 3: Create DBT PROJECT from Stage
-- =====================================================
CREATE OR REPLACE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    FROM @DBT_CONTROL.DBT_PROJECT_STAGE
    SUBDIRECTORY = 'dbt_dev'
    CONNECTION = (
        DATABASE => 'COMMON_TENANT_DEV',
        WAREHOUSE => 'WH_ETL_DEV',
        ROLE => 'R_DEPLOYMENT_ADMIN_DEV'
    );

-- Verify project
DESCRIBE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT;

-- =====================================================
-- STEP 4: Test dbt Commands
-- =====================================================

-- Parse (validate syntax)
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'parse';

-- Compile
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'compile --select newsletter_category';

-- Run models
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'run --select newsletter_category';

-- Run tests
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'test --select newsletter_category';

-- =====================================================
-- STEP 5: Create Wrapper Procedure for Airflow
-- =====================================================
CREATE OR REPLACE PROCEDURE DBT_CONTROL.PRC_EXECUTE_DBT(PARAMS_JSON VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    dbt_command VARCHAR;
    dbt_select VARCHAR;
BEGIN
    dbt_command := COALESCE(PARSE_JSON(:PARAMS_JSON):command::VARCHAR, 'run');
    dbt_select := COALESCE(PARSE_JSON(:PARAMS_JSON):select::VARCHAR, 'newsletter_category');
    
    EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
        ARGS => :dbt_command || ' --select ' || :dbt_select;
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'success',
        'command', :dbt_command,
        'select', :dbt_select
    );
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'error',
            'error', SQLERRM
        );
END;
$$;

-- =====================================================
-- STEP 6: Test from Airflow perspective
-- =====================================================

-- This is what Airflow will call:
CALL DBT_CONTROL.PRC_EXECUTE_DBT('{"command": "run", "select": "newsletter_category"}');
CALL DBT_CONTROL.PRC_EXECUTE_DBT('{"command": "test", "select": "newsletter_category"}');

-- =====================================================
-- TO UPDATE DBT PROJECT (after code changes):
-- =====================================================
-- 1. Re-upload files to stage:
--    COPY FILES INTO @DBT_CONTROL.DBT_PROJECT_STAGE/dbt_dev/ FROM 'snow://workspace/...'
--
-- 2. Recreate project:
--    CREATE OR REPLACE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT ...
