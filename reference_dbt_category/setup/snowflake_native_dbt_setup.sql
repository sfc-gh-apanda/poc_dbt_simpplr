-- =====================================================
-- SNOWFLAKE NATIVE DBT PROJECT SETUP
-- Newsletter Category Models
-- =====================================================
-- This script sets up dbt to run INSIDE Snowflake
-- No need to install dbt in Airflow - just call SQL!
-- =====================================================

USE ROLE ACCOUNTADMIN;  -- Required for API integration
USE DATABASE COMMON_TENANT_DEV;
USE WAREHOUSE WH_ETL_DEV;

-- =====================================================
-- STEP 1: Create Secret for Git Credentials
-- =====================================================
-- Option A: GitHub Personal Access Token (PAT)
CREATE OR REPLACE SECRET DBT_CONTROL.GITHUB_PAT_SECRET
    TYPE = PASSWORD
    USERNAME = 'your-github-username'
    PASSWORD = 'ghp_xxxxxxxxxxxxxxxxxxxx';  -- Your GitHub PAT

-- Option B: If using SSH key (alternative)
-- CREATE OR REPLACE SECRET DBT_CONTROL.GITHUB_SSH_SECRET
--     TYPE = GENERIC_STRING
--     SECRET_STRING = '-----BEGIN OPENSSH PRIVATE KEY-----...';

-- Grant usage to deployment role
GRANT USAGE ON SECRET DBT_CONTROL.GITHUB_PAT_SECRET TO ROLE R_DEPLOYMENT_ADMIN_DEV;


-- =====================================================
-- STEP 2: Create API Integration for GitHub
-- =====================================================
CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
    API_PROVIDER = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = ('https://github.com/your-org/')  -- Your GitHub org
    ALLOWED_AUTHENTICATION_SECRETS = (DBT_CONTROL.GITHUB_PAT_SECRET)
    ENABLED = TRUE;

-- Grant usage
GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION TO ROLE R_DEPLOYMENT_ADMIN_DEV;


-- =====================================================
-- STEP 3: Create Git Repository Connection
-- =====================================================
USE ROLE R_DEPLOYMENT_ADMIN_DEV;
USE DATABASE COMMON_TENANT_DEV;

CREATE OR REPLACE GIT REPOSITORY DBT_CONTROL.DBT_NEWSLETTER_REPO
    API_INTEGRATION = GITHUB_API_INTEGRATION
    GIT_CREDENTIALS = DBT_CONTROL.GITHUB_PAT_SECRET
    ORIGIN = 'https://github.com/your-org/your-dbt-repo.git';

-- Fetch latest from remote
ALTER GIT REPOSITORY DBT_CONTROL.DBT_NEWSLETTER_REPO FETCH;

-- List branches
SHOW GIT BRANCHES IN DBT_CONTROL.DBT_NEWSLETTER_REPO;

-- List files in repo (verify structure)
LS @DBT_CONTROL.DBT_NEWSLETTER_REPO/branches/main/;


-- =====================================================
-- STEP 4: Create DBT PROJECT
-- =====================================================
CREATE OR REPLACE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    FROM GIT REPOSITORY DBT_CONTROL.DBT_NEWSLETTER_REPO
    GIT_BRANCH = 'main'
    SUBDIRECTORY = 'dbt_dev'  -- Path to dbt project in repo
    CONNECTION = (
        DATABASE => 'COMMON_TENANT_DEV',
        SCHEMA => 'DBT',
        WAREHOUSE => 'WH_ETL_DEV',
        ROLE => 'R_DEPLOYMENT_ADMIN_DEV'
    );

-- Verify project
DESCRIBE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT;


-- =====================================================
-- STEP 5: Test dbt Commands
-- =====================================================

-- Parse project (validates syntax)
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'parse';

-- Compile models
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'compile --select newsletter_category';

-- Run models (actual execution)
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'run --select newsletter_category';

-- Run tests
EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT
    ARGS => 'test --select newsletter_category';


-- =====================================================
-- STEP 6: Create Wrapper Procedure for Airflow
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
    result VARIANT;
BEGIN
    -- Parse parameters
    dbt_command := COALESCE(PARSE_JSON(:PARAMS_JSON):command::VARCHAR, 'run');
    dbt_select := COALESCE(PARSE_JSON(:PARAMS_JSON):select::VARCHAR, 'newsletter_category');
    
    -- Execute dbt
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

-- Test wrapper
CALL DBT_CONTROL.PRC_EXECUTE_DBT('{"command": "compile", "select": "newsletter_category"}');


-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================
GRANT USAGE ON PROCEDURE DBT_CONTROL.PRC_EXECUTE_DBT(VARCHAR) TO ROLE R_DEPLOYMENT_ADMIN_DEV;
GRANT USAGE ON GIT REPOSITORY DBT_CONTROL.DBT_NEWSLETTER_REPO TO ROLE R_DEPLOYMENT_ADMIN_DEV;
GRANT USAGE ON DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT TO ROLE R_DEPLOYMENT_ADMIN_DEV;


-- =====================================================
-- USEFUL COMMANDS
-- =====================================================

-- Refresh from Git (pull latest)
-- ALTER GIT REPOSITORY DBT_CONTROL.DBT_NEWSLETTER_REPO FETCH;

-- List available models
-- EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT ARGS => 'list';

-- Generate docs
-- EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT ARGS => 'docs generate';

-- Run specific model
-- EXECUTE DBT PROJECT DBT_CONTROL.DBT_NEWSLETTER_PROJECT ARGS => 'run --select wrk_newsletter_category';
