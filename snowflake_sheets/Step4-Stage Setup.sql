USE ROLE tb_data_engineer;
-- USE DATABASE tb_101;
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_FRANCHISE_AGENT_MARTS;

CREATE STAGE IF NOT EXISTS semantic_models
    COMMENT = 'Stage for Cortex Analyst semantic model YAML files';

-- Then use SnowSQL or Snowsight's Upload button:
-- ```bash
-- # From your terminal with SnowSQL installed:
-- snowsql -a <account> -u <user> -d tb_101 -s franchise_agent_marts \
--   -q "PUT file://franchise_revenue_semantic_model.yaml @semantic_models AUTO_COMPRESS=false OVERWRITE=true"
-- ```

-- Or in Snowsight: Data → Databases → tb_101 → franchise_agent_marts → Stages → semantic_models → Upload.