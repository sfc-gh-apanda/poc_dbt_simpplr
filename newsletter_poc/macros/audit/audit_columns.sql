{#
═══════════════════════════════════════════════════════════════════════════════
AUDIT COLUMNS MACROS
═══════════════════════════════════════════════════════════════════════════════

Adds standardized dbt tracking columns alongside existing bookkeeping columns.
Existing Scala-compatible columns (created_by, created_datetime, active_flag, etc.)
are preserved in each model; these macros ADD dbt-specific tracking on top.

Columns Generated:
  - dbt_run_id        : Unique dbt invocation ID
  - dbt_batch_id      : Unique per-model-per-run identifier
  - dbt_loaded_at     : Timestamp when dbt loaded the record
  - dbt_source_model  : Name of the dbt model that produced the record
  - dbt_environment   : Target environment (dev / prod)

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro audit_columns() %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                       AS dbt_loaded_at,
    '{{ invocation_id }}'::VARCHAR(50)                       AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    '{{ this.name }}'::VARCHAR(100)                          AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20)                         AS dbt_environment
{% endmacro %}


{% macro audit_columns_incremental(existing_alias='existing') %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                       AS dbt_loaded_at,
    '{{ invocation_id }}'::VARCHAR(50)                       AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    '{{ this.name }}'::VARCHAR(100)                          AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20)                         AS dbt_environment
{% endmacro %}
