# 🚀 dbt Pipeline Health Monitor
### Snowflake-Native AI Ops | Tasty Bytes | dbt CLI

Stop digging through Airflow logs or squinting at raw SQL history. This project transforms your Snowflake account usage data into a searchable, AI-powered health assistant. 

**Ask your pipeline questions in plain English:**
* *"Which dbt models are getting slower month-over-month?"*
* *"What's the most expensive model in the pipeline right now?"*
* *"Show me all timeout failures from the last week with suggested fixes."*

---

## 💡 The Core Concept
Most monitoring tools are either basic dashboards (no context) or external SaaS platforms (data leaves your VPC). This stays **100% inside Snowflake**, adding three unique AI layers:

1.  **Automated Classification:** `AI_CLASSIFY` buckets every failure into categories like *Schema Drift*, *Logic Error*, or *Resource Constraint*.
2.  **Smart Fixes:** `AI_COMPLETE` generates human-readable fix suggestions for every incident.
3.  **Cortex Agent:** A chat interface that lets any engineer query the health mart without writing a single line of SQL.

---

## 🏗️ Architecture

```text
SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
           │
           ▼
┌──────────────────────────────────────┐
│  PHASE 1: dbt Macro Enrichment       │
│  Injects model names & metadata      │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│  PHASE 2 & 3: AI Enrichment Layer    │
│  AI_CLASSIFY: Categorizes failures   │
│  AI_COMPLETE: Generates fix text     │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│  PHASE 4 & 5: Data Serving           │
│  Structured Mart + Cortex Search     │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│  PHASE 6: Cortex Agent               │
│  Natural Language Interface          │
└──────────────────────────────────────┘
```

---

## Prerequisites

Before you start, ensure you have:
- Snowflake Account: Trial account with Tasty Bytes data loaded.
- Permissions: ACCOUNTADMIN or GOVERNANCE_VIEWER (to access ACCOUNT_USAGE).
- dbt CLI: Installed and configured with a working profile.
- Warehouse: A standard tb_de_wh warehouse available.

---
## Getting Started

1. Initialize dbt
Install dependencies and test your connection:
```
dbt deps
dbt run
dbt test
```
2. Configure Query Tagging
By default, dbt tags are sparse. This project uses a set_query_tag macro to inject JSON metadata.

Note on Latency: ACCOUNT_USAGE.QUERY_HISTORY has up to 2 hours of latency. For near-real-time testing, use INFORMATION_SCHEMA, though it has shorter retention.

3. Deploy the AI Layer
Follow the phases in the project to deploy the enrichment logic:

Staging: Parse the JSON query tags into typed columns.
AI Enrichment: Use AI_CLASSIFY and AI_COMPLETE to build the incident_summary.
Automation: Set up a Snowflake Task to refresh the health mart every hour.

---

## Test the Agent - At the end

Open **Snowsight → AI & ML → Snowflake Intelligence**, select `dbt_pipeline_health_agent`.

**Structured questions (Cortex Analyst):**
```
Which model has the lowest success rate?
What are the top 5 most expensive models by data scanned?
Show me models that are getting slower compared to last month.
How many timeout failures happened in the last 7 days?
Give me a full health ranking of all models.
What is the health score for fct_franchise_revenue?
```

**Incident questions (Cortex Search):**
```
What failed last night?
Are there any schema drift errors?
Show me fix suggestions for timeout failures.
What went wrong with the stg_order_header model?
Which failures were caused by the warehouse being too small?
```

**Cross-domain questions (both tools):**
```
Which model has the most failures and what does it say went wrong?
Tell me everything about the unhealthiest model in the pipeline.
Show me models with both low success rates AND spill incidents — what are the suggested fixes?
Which failures this week have not been fixed based on the suggestions given?
```

---

## Summary: what each phase teaches you

| Phase | Snowflake concept | dbt concept |
|---|---|---|
| 1 — Query tags | JSON query tags, ACCOUNT_USAGE | `set_query_tag` macro, packages |
| 2 — Staging model | ACCOUNT_USAGE latency, PARSE_JSON | Incremental models, source freshness |
| 3 — AI enrichment | AI_CLASSIFY, AI_COMPLETE, text assembly | Post-dbt SQL enrichment pattern |
| 4 — Mart models | Aggregation patterns, health scoring | Multi-table marts, metric computation |
| 5 — Cortex Search | Search service creation, attribute filtering | None |
| 6 — Semantic model | Cortex Analyst YAML, verified queries | None |
| 7 — Snowflake Task | Task scheduling, stored procedures | Operational dbt pattern |
| 8 — Cortex Agent | Agent routing, dual-tool orchestration | None |
| 9 — Intelligence UI | Cross-domain questioning | None |

---


