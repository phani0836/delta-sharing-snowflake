# Securely Read Databricks Delta Shares from Snowflake

This repository supports the blog post:  
**“How to Securely read Databricks Delta Shares from Snowflake; with Snowflake Artifact Repository”**  
📖 [Read the full blog here](#) <!-- Replace # with actual blog URL -->

## 📌 Overview

This project demonstrates how to securely consume **Databricks Delta Share tables** in **Snowflake** using:

- ✅ Snowflake External Access Integration  
- ✅ Network Access Rules  
- ✅ Snowflake Artifact Repository  
- ✅ Snowpark Stored Procedures  
- ✅ Delta Sharing Python Library  
- ✅ Incremental ingestion using timestamp-based CDC

---

## 📁 Repository Structure

```plaintext
secure-delta-share-snowflake/
├── snowflake/
│   ├── network_rules.sql                  # Network and external access setup
│   ├── external_access_integration.sql   # External access integration definition
│   ├── stored_procedure_delta_share.sql  # Snowpark stored procedure (initial + CDC load)
│   └── stage_config_file.sql             # JSON file format and staging setup
├── notebook/
│   └── snowflake_notebook_delta_share.ipynb  # Notebook-based alternative to stored procedure
└── README.md
