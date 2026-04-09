USE ROLE accountadmin;
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA 
SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR_MARTS;

CREATE OR REPLACE AGENT dbt_pipeline_health_agent
  COMMENT = 'dbt Pipeline Health Monitor — Tasty Bytes'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  instructions:
    system: "I am a dbt pipeline health agent for the Tasty Bytes project. I can answer questions about model performance, failure rates, cost, and specific incident details. I combine structured metrics with AI-classified incident records."
    orchestration: "When asked about aggregated metrics — failure counts, average runtimes, success rates, health scores, slowest models, most expensive models — use the Cortex Analyst tool. When asked about specific incidents, error messages, fix suggestions, or qualitative questions like 'what went wrong with model X last Tuesday?' — use the Cortex Search tool. When a question spans both (e.g. 'show me the worst model and tell me what failed') — use both and clearly attribute each part of the answer."
    response: "Use technical but clear language. When showing durations, express them in seconds or minutes as appropriate. When showing failure categories, explain them briefly."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "pipeline_metrics_analyst"
        description: "Answers structured questions about dbt pipeline performance: failure rates, run durations, cost metrics, health scores, and trend analysis across models."
    - tool_spec:
        type: "cortex_search"
        name: "incident_search"
        description: "Searches AI-classified incident records with fix suggestions. Use for questions about specific failures, error types, recommended fixes, or incidents for a specific model."

  tool_resources:
    pipeline_metrics_analyst:
      semantic_model_file: "@SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR_MARTS.semantic_models/dbt_pipeline_health_model.yaml"
    incident_search:
      name: "SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR_MARTS.dbt_incident_search"
      max_results: "5"
  $$;

GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR_MARTS.dbt_pipeline_health_agent
    TO ROLE tb_data_engineer;