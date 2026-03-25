{% macro get_batch_context() %}
    {% set default_ctx = {
        'batch_id': 0,
        'batch_start_time': '2000-01-01 00:00:00',
        'data_process_start_time': '2000-01-01 00:00:00',
        'data_process_end_time': '2100-01-01 00:00:00',
        'is_full_load': false,
        'full_load_transformations': ''
    } %}
    
    {% if execute %}
        {% set query %}
            SELECT 
                BATCH_RUN_ID,
                TO_VARCHAR(BATCH_START_TIME, 'YYYY-MM-DD HH24:MI:SS'),
                TO_VARCHAR(DATA_PROCESS_START_TIME, 'YYYY-MM-DD HH24:MI:SS'),
                TO_VARCHAR(DATA_PROCESS_END_TIME, 'YYYY-MM-DD HH24:MI:SS'),
                FULL_LOAD,
                PROCESS_RUNNING_IN_FULL_LOAD
            FROM {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_BATCH_RUN
            WHERE ACTIVE_FLAG = TRUE
        {% endset %}
        
        {% set result = run_query(query) %}
        
        {% if result and result.rows | length > 0 %}
            {{ return({
                'batch_id': result.rows[0][0],
                'batch_start_time': result.rows[0][1],
                'data_process_start_time': result.rows[0][2],
                'data_process_end_time': result.rows[0][3],
                'is_full_load': result.rows[0][4],
                'full_load_transformations': result.rows[0][5] or ''
            }) }}
        {% else %}
            {% if flags.WHICH in ['compile', 'parse', 'generate'] %}
                {{ return(default_ctx) }}
            {% else %}
                {{ exceptions.raise_compiler_error("No active batch found in DBT_BATCH_RUN. Start batch first.") }}
            {% endif %}
        {% endif %}
    {% else %}
        {{ return(default_ctx) }}
    {% endif %}
{% endmacro %}


{% macro get_batch_id() %}
    {% set ctx = get_batch_context() %}
    {{ return(ctx['batch_id']) }}
{% endmacro %}


{% macro is_full_load(transformation_name=none) %}
    {% set ctx = get_batch_context() %}
    
    {% if ctx['is_full_load'] %}
        {{ return(true) }}
    {% endif %}
    
    {% if transformation_name and ctx['full_load_transformations'] %}
        {% set full_load_list = ctx['full_load_transformations'].split(',') | map('trim') | list %}
        {% if transformation_name in full_load_list %}
            {{ return(true) }}
        {% endif %}
    {% endif %}
    
    {{ return(false) }}
{% endmacro %}


{% macro get_watermark_start(transformation_name=none) %}
    {% set ctx = get_batch_context() %}
    
    {% if is_full_load(transformation_name) %}
        {{ return('2000-01-01 00:00:00') }}
    {% endif %}
    
    {{ return(ctx['data_process_start_time']) }}
{% endmacro %}


{% macro get_watermark_end() %}
    {% set ctx = get_batch_context() %}
    {{ return(ctx['data_process_end_time']) }}
{% endmacro %}


{% macro get_next_process_order(batch_id) %}
    {% set query %}
        SELECT COALESCE(MAX(PROCESS_ORDER), 0) + 1 
        FROM {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_PROCESS_RUN 
        WHERE BATCH_RUN_ID = {{ batch_id }}
    {% endset %}
    {% set result = run_query(query) %}
    {{ return(result.rows[0][0] if result and result.rows | length > 0 else 1) }}
{% endmacro %}


{% macro get_entity_id(transformation_name) %}
    {% set query %}
        SELECT ID 
        FROM {{ target.database }}.DBT_CONFIG.DBT_ENTITY_MAPPING 
        WHERE DBT_MODEL_NAME = '{{ transformation_name }}' 
          AND ACTIVE_FLAG = TRUE 
        LIMIT 1
    {% endset %}
    {% set result = run_query(query) %}
    {{ return(result.rows[0][0] if result and result.rows | length > 0 else none) }}
{% endmacro %}


{% macro log_process_start(transformation_name, process_step) %}
    {% if execute %}
        {% set batch_id = get_batch_id() %}
        
        {% if batch_id %}
            {% set entity_id = get_entity_id(transformation_name) %}
            {% set process_order = get_next_process_order(batch_id) %}
            
            {% set insert_sql %}
                INSERT INTO {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_PROCESS_RUN 
                (BATCH_RUN_ID, ENTITY_ID, ENTITY_NAME, PROCESS_STEP, PROCESS_ORDER, PROCESS_STATUS, PROCESS_START_TIME, CREATED_BY)
                VALUES (
                    {{ batch_id }},
                    {{ entity_id if entity_id else 'NULL' }},
                    '{{ transformation_name }}',
                    '{{ process_step }}',
                    {{ process_order }},
                    'RUNNING',
                    CURRENT_TIMESTAMP(),
                    CURRENT_USER()
                )
            {% endset %}
            {% do run_query(insert_sql) %}
            {% do run_query("COMMIT") %}
            {{ log("  → Started: " ~ transformation_name ~ "/" ~ process_step, info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro log_process_end(transformation_name, process_step) %}
    {% if execute %}
        {% set batch_id = get_batch_id() %}
        
        {% if batch_id %}
            {% set row_count = 0 %}
            {% set count_sql %}SELECT COUNT(*) FROM {{ this }}{% endset %}
            {% set count_result = run_query(count_sql) %}
            {% if count_result and count_result.rows | length > 0 %}
                {% set row_count = count_result.rows[0][0] %}
            {% endif %}
            
            {% set update_sql %}
                UPDATE {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_PROCESS_RUN
                SET PROCESS_STATUS = 'COMPLETED',
                    PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                    PROCESS_DURATION_SECONDS = DATEDIFF('SECOND', PROCESS_START_TIME, CURRENT_TIMESTAMP()),
                    ROWS_PROCESSED = {{ row_count }},
                    UPDATED_DATE = CURRENT_TIMESTAMP(),
                    UPDATED_BY = CURRENT_USER()
                WHERE BATCH_RUN_ID = {{ batch_id }}
                  AND ENTITY_NAME = '{{ transformation_name }}'
                  AND PROCESS_STEP = '{{ process_step }}'
                  AND PROCESS_STATUS = 'RUNNING'
            {% endset %}
            {% do run_query(update_sql) %}
            {% do run_query("COMMIT") %}
            {{ log("  ✓ Completed: " ~ transformation_name ~ "/" ~ process_step ~ " (" ~ row_count ~ " rows)", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro mark_failed_processes() %}
    {% if execute and flags.WHICH == 'run' %}
        {% set query %}
            SELECT BATCH_RUN_ID FROM {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_BATCH_RUN
            WHERE ACTIVE_FLAG = TRUE
        {% endset %}
        {% set result = run_query(query) %}
        
        {% if result and result.rows | length > 0 %}
            {% set batch_id = result.rows[0][0] %}
            
            {% set update_sql %}
                UPDATE {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_PROCESS_RUN
                SET PROCESS_STATUS = 'FAILED',
                    PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                    PROCESS_DURATION_SECONDS = DATEDIFF('SECOND', PROCESS_START_TIME, CURRENT_TIMESTAMP()),
                    ERROR_MESSAGE = 'Process did not complete',
                    UPDATED_DATE = CURRENT_TIMESTAMP(),
                    UPDATED_BY = CURRENT_USER()
                WHERE BATCH_RUN_ID = {{ batch_id }}
                  AND PROCESS_STATUS = 'RUNNING'
            {% endset %}
            {% do run_query(update_sql) %}
            {% do run_query("COMMIT") %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro check_batch_active() %}
    {% if execute and flags.WHICH == 'run' %}
        {% set query %}
            SELECT BATCH_RUN_ID, FULL_LOAD
            FROM {{ target.database }}.DBT_EXECUTION_RUN_STATS.DBT_BATCH_RUN
            WHERE ACTIVE_FLAG = TRUE
        {% endset %}
        {% set result = run_query(query) %}
        
        {% if result and result.rows | length > 0 %}
            {% if result.rows[0][1] %}
                {{ log("📦 Full Load", info=True) }}
            {% else %}
                {{ log("📊 Delta Load", info=True) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
