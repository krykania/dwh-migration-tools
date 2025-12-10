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
 WITH
  tables AS (
    SELECT IS_ICEBERG, IS_DYNAMIC, IS_HYBRID, AUTO_CLUSTERING_ON, RETENTION_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
  ),
  columns AS (
    SELECT DATA_TYPE, DATETIME_PRECISION
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
  ),
  procedures AS (
    SELECT PACKAGES, PROCEDURE_LANGUAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES
    WHERE DELETED IS NULL
  ),
  functions AS (
    SELECT PACKAGES, FUNCTION_LANGUAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS
    WHERE DELETED IS NULL
  )
  -- iceberg tables
  SELECT 'storage_table_layout', 'iceberg_columns', COUNT(*), ''
  FROM tables WHERE IS_ICEBERG = 'YES'

  UNION ALL
  -- dynamic tables
  SELECT 'storage_table_layout', 'dynamic_table_columns', COUNT(*), ''
  FROM tables WHERE IS_DYNAMIC = 'YES'

  UNION ALL
  -- hybrid tables
  SELECT 'storage_table_layout', 'hybrid_table_columns', COUNT(*), ''
  FROM tables WHERE IS_HYBRID = 'YES'

  UNION ALL
  -- automatic clustering tables
  SELECT 'services', 'automatic_clustering', COUNT(*), ''
  FROM tables WHERE AUTO_CLUSTERING_ON = 'YES'

  UNION ALL
  -- time travel, retention period >= 7 and <= 14
  SELECT'services', 'time_travel_between_7d_14d', COUNT(*), ''
  FROM tables WHERE RETENTION_TIME >= 7 AND RETENTION_TIME <= 14

  UNION ALL
  -- time travel, retention period >= 14
  SELECT'services', 'time_travel_gt_14d', COUNT(*), ''
  FROM tables WHERE RETENTION_TIME >= 7 AND RETENTION_TIME >= 14

  UNION ALL
  -- parquet file format
  SELECT 'storage', 'parquet_format', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS
  WHERE DELETED IS NULL AND UPPER(FILE_FORMAT_TYPE) = 'PARQUET'

  UNION ALL
  -- masking policies
  SELECT 'security', 'masking_policies', COUNT(*), ''
  FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
  WHERE POLICY_KIND = 'MASKING_POLICY'

  UNION ALL
  -- secure views
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
  SELECT 'service', 'snowpark_procedures', COUNT(*), 'PROCEDURES'
  FROM procedures
    WHERE PROCEDURE_LANGUAGE IN ('PYTHON','JAVA','SCALA')
      AND (PACKAGES LIKE '%snowflake-snowpark-python%' OR PACKAGES LIKE '%com.snowflake:snowpark%')
  UNION ALL
  SELECT 'service', 'snowpark_functions', COUNT(*), 'FUNCTIONS'
  FROM functions
    WHERE FUNCTION_LANGUAGE IN ('PYTHON','JAVA','SCALA')
      AND (PACKAGES LIKE '%snowflake-snowpark-python%' OR PACKAGES LIKE '%com.snowflake:snowpark%')

  UNION ALL
  -- Zero copy replication
  SELECT 'service', 'zero_copy_replication', COUNT(*), 'CLONES'
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
  WHERE DELETED IS NULL

  UNION ALL
  -- SP - Javascript
  SELECT 'sql', 'stored_procedures_javascript', COUNT(*), ''
  FROM procedures
  WHERE PROCEDURE_LANGUAGE = 'JAVASCRIPT'

  UNION ALL
  -- SP - Python
  SELECT 'sql', 'stored_procedures_python', COUNT(*), ''
  FROM procedures
  WHERE PROCEDURE_LANGUAGE = 'PYTHON'

  UNION ALL
  -- SP - Java
  SELECT 'sql', 'stored_procedures_java', COUNT(*), ''
  FROM procedures
  WHERE PROCEDURE_LANGUAGE = 'JAVA'

  UNION ALL
  -- SP - SQL
  SELECT 'sql', 'stored_procedures_sql', COUNT(*), ''
  FROM procedures
  WHERE PROCEDURE_LANGUAGE = 'SQL'

  UNION ALL
  -- function - Javascript
  SELECT 'sql', 'functions_javascript', COUNT(*), ''
  FROM functions
  WHERE FUNCTION_LANGUAGE = 'JAVASCRIPT'

  UNION ALL
  -- function - Python
  SELECT 'sql', 'functions_python', COUNT(*), ''
  FROM functions
  WHERE FUNCTION_LANGUAGE = 'PYTHON'

  UNION ALL
  -- function - Java
  SELECT 'sql', 'functions_java', COUNT(*), ''
  FROM functions
  WHERE FUNCTION_LANGUAGE = 'JAVA'

  UNION ALL
  -- function - SQL
  SELECT 'sql', 'functions_sql', COUNT(*), ''
  FROM functions
  WHERE FUNCTION_LANGUAGE = 'SQL'

  UNION ALL
  -- data types - variant
  SELECT 'data_types', 'variant', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'VARIANT'

  UNION ALL
  -- data types - object
  SELECT 'data_types', 'object', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'OBJECT'

  UNION ALL
  -- data types - vector
  SELECT 'data_types', 'vector', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'VECTOR'

  UNION ALL
  -- data types - geometry
  SELECT 'data_types', 'geometry', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'GEOMETRY'

  UNION ALL
  -- data types - timestamp_ltz
  SELECT 'data_types', 'timestamp_ltz', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'TIMESTAMP_LTZ'

  UNION ALL
  -- data types - timestamp_ntz
  SELECT 'data_types', 'timestamp_ntz', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'TIMESTAMP_NTZ'

  UNION ALL
  -- data types - timestamp_tz
  SELECT 'data_types', 'timestamp_tz', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE = 'TIMESTAMP_TZ'

  UNION ALL
  -- data types - timestamp with nanoseconds
  SELECT 'data_types', 'timestamp_nano', COUNT(*), ''
  FROM columns
  WHERE DATA_TYPE LIKE 'TIMESTAMP%' AND DATETIME_PRECISION > 6;
