USE ROLE accountadmin;

-- Grant your data engineer role access to account usage
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE tb_data_engineer;