-- Copyright 2022-2025 Google LLC
-- Copyright 2013-2021 CompilerWorks
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
DECLARE
  -- Variables to hold the Query IDs
  account_usage_query_id VARCHAR;
  show_tables_query_id VARCHAR;
  show_dbt_projects_query_id VARCHAR;
  show_warehouses_query_id VARCHAR;
  show_tasks_query_id VARCHAR;
  final_result RESULTSET;
BEGIN
  -- ACCOUNT_USAGE
  WITH base AS (
    SELECT t.IS_ICEBERG, t.IS_DYNAMIC, t.IS_HYBRID, t.AUTO_CLUSTERING_ON, t.CLUSTERING_KEY, t.RETENTION_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
    WHERE t.DELETED IS NULL
  ),
  clustering_base AS (
    SELECT * FROM base WHERE CLUSTERING_KEY IS NOT NULL
  ),
  clustering_no_prefix AS (
    -- Remove 'LINEAR(' from the front
    SELECT
      CLUSTERING_KEY,
      REGEXP_REPLACE(CLUSTERING_KEY, '^\\s*LINEAR\\s*\\(\\s*', '', 1, 1, 'i') AS s1
    FROM clustering_base
  ),
  clustering_inner_text AS (
    -- remove trailing ')' + trailing spaces/newline
    SELECT
      CLUSTERING_KEY,
      REGEXP_REPLACE(s1, '\\)\\s*$', '', 1, 1, 'i') AS body
    FROM clustering_no_prefix
  ),
  clustering_compact AS (
    -- remove ALL white characters and quotation marks to compare "bare" characters
    SELECT
      CLUSTERING_KEY, body,
      REGEXP_REPLACE(body, '[\\s"]', '', 1, 0, 'is') AS body_compact
    FROM clustering_inner_text
  ),
  clustering_classified AS (
    SELECT
      CLUSTERING_KEY,
      body,
      CASE
        -- if ONLY A–Z, 0–9, _, $, . and , remain after compression, then it is a clean list of columns
        WHEN REGEXP_LIKE(body_compact, '^[A-Z0-9_\\$\\.,]+$', 'i')
          THEN 'BY_COLUMNS'
        ELSE 'BY_EXPRESSION'
      END AS key_type
    FROM clustering_compact
  )
  -- iceberg tables
  SELECT
    'storage_table_layout',
    'iceberg_columns',
     COUNT(*),
     ''
  FROM base WHERE IS_ICEBERG = 'YES'
  UNION ALL
  -- dynamic tables
  SELECT 'storage_table_layout', 'dynamic_table_columns', COUNT(*), ''
  FROM base WHERE IS_DYNAMIC = 'YES'
  UNION ALL
  -- hybrid tables
  SELECT 'storage_table_layout', 'hybrid_table_columns', COUNT(*), ''
  FROM base WHERE IS_HYBRID = 'YES'
  UNION ALL
  -- automatic clustering tables
  SELECT 'services', 'automatic_clustering', COUNT(*), ''
  FROM base WHERE AUTO_CLUSTERING_ON = 'YES'
  UNION ALL
  -- clustering by columns
  SELECT 'storage_table_layout', 'clustering_by_columns', COUNT(*), ''
  FROM clustering_classified WHERE key_type='BY_COLUMNS'
  UNION ALL
  -- clustering by expressions
  SELECT'storage_table_layout', 'clustering_by_expressions', COUNT(*), ''
  FROM clustering_classified WHERE key_type='BY_EXPRESSION'
  UNION ALL
  -- time travel, retention period >= 7 and <= 14
  SELECT'services', 'time_travel_between_7d_14d', COUNT(*), ''
  FROM base WHERE RETENTION_TIME >= 7 AND RETENTION_TIME <= 14
  UNION ALL
  -- time travel, retention period >= 14
  SELECT'services', 'time_travel_gt_14d', COUNT(*), ''
  FROM base WHERE RETENTION_TIME >= 7 AND RETENTION_TIME >= 14
  UNION ALL
  -- parquet file format
  SELECT 'storage', 'parquet_format', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS
  WHERE DELETED IS NULL AND UPPER(FILE_FORMAT_TYPE) = 'PARQUET'
  -- masking policies
  UNION ALL
  SELECT 'security', 'masking_policies', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
  WHERE POLICY_KIND = 'MASKING_POLICY'
  -- secure views
  UNION ALL
  SELECT 'security', 'secure_views', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS
  WHERE DELETED IS NULL AND IS_SECURE = 'YES'
  UNION ALL
  -- custom roles
  SELECT 'security', 'custom_roles', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
  WHERE DELETED_ON IS NULL
   AND UPPER(NAME) NOT IN ('ACCOUNTADMIN','SYSADMIN','SECURITYADMIN','USERADMIN','ORGADMIN','PUBLIC')
  UNION ALL
  -- Snowpark Usage
  SELECT
    'service',
    'snowpark_procedures',
    COUNT(*),
    'PROCEDURES'
  FROM SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES
    WHERE PROCEDURE_LANGUAGE IN ('PYTHON','JAVA','SCALA')
      AND (PACKAGES LIKE '%snowflake-snowpark-python%' OR PACKAGES LIKE '%com.snowflake:snowpark%')
  UNION ALL
  SELECT
    'service',
    'snowpark_functions',
    COUNT(*),
    'FUNCTIONS'
  FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS
    WHERE FUNCTION_LANGUAGE IN ('PYTHON','JAVA','SCALA')
      AND (PACKAGES LIKE '%snowflake-snowpark-python%' OR PACKAGES LIKE '%com.snowflake:snowpark%')
  UNION ALL
  -- Zero copy replication
  SELECT
    'service',
    'zero_copy_replication',
    COUNT(*),
    'CLONES'
  FROM snowflake.account_usage.table_storage_metrics t1
  JOIN (
    SELECT CLONE_GROUP_ID, MIN(TABLE_CREATED) AS min_created
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE CLONE_GROUP_ID IS NOT NULL
    GROUP BY CLONE_GROUP_ID
  ) t2 ON t1.CLONE_GROUP_ID = t2.CLONE_GROUP_ID AND t1.TABLE_CREATED > t2.min_created
  WHERE t1.DELETED = FALSE
  UNION ALL
  -- Snowpipe
  SELECT 'etl', 'snowpipe', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES
  WHERE DELETED IS NULL;

  account_usage_query_id := LAST_QUERY_ID();

  -- Show tables
  SHOW TABLES IN ACCOUNT;

  -- Search optimization detection, in some editions, the snowflake flag is not available.
  WITH flattened_show_tables AS (
    SELECT UPPER(flat.key::string) AS column_name,
      flat.value::string AS column_value
    FROM (
      SELECT OBJECT_CONSTRUCT(*) AS table_row_object
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    ) tab,
    LATERAL FLATTEN(INPUT => tab.table_row_object) flat
  ),
  aggregated AS (
    SELECT
      MAX(IFF(column_name = 'SEARCH_OPTIMIZATION', 1, 0)) AS has_search_optimization_column,
      SUM(IFF(column_name = 'SEARCH_OPTIMIZATION' AND column_value = 'ON', 1, 0)) AS search_optimization_on_count
    FROM flattened_show_tables
  )
  SELECT 'table', 'search_optimization',  IFF(has_search_optimization_column = 1, search_optimization_on_count, 0), IFF(has_search_optimization_column = 1, '', 'no_column')
  FROM aggregated;

  show_tables_query_id := LAST_QUERY_ID();

  SHOW DBT PROJECTS IN ACCOUNT;

  -- DBT Projects
  SELECT 'etl', 'dbt_projects', COUNT(*), 'PROJECTS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_dbt_projects_query_id := LAST_QUERY_ID();

  SHOW WAREHOUSES IN ACCOUNT;

  -- query acceleration
  SELECT 'service', 'query_acceleration', COUNT(*), 'WAREHOUSES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE UPPER('enable_query_acceleration') = 'true'
  UNION ALL
  -- Gen2 warehouses
  SELECT 'service', 'gen2_warehouses', COUNT(*), 'WAREHOUSES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE UPPER('resource_constraint') = 'STANDARD_GEN_2'
  UNION ALL
  -- snowpark-optimized
  SELECT 'service', 'snowpark_optimized_warehouses', COUNT(*), 'WAREHOUSES'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "type" = 'SNOWPARK-OPTIMIZED';

  show_warehouses_query_id := LAST_QUERY_ID();

  SHOW TASKS IN ACCOUNT;

  SELECT 'service', 'tasks', COUNT(*), 'TASKS'
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  show_tasks_query_id := LAST_QUERY_ID();

  final_result := (
    SELECT * FROM TABLE(RESULT_SCAN(:account_usage_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_tables_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_dbt_projects_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_warehouses_query_id))
    UNION ALL
    SELECT * FROM TABLE(RESULT_SCAN(:show_tasks_query_id))
  );

  RETURN TABLE(final_result);
END;
