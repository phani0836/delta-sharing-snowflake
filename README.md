# Securely Read Databricks Delta Shares from Snowflake

This repository supports the blog post:  
**“How to Securely read Databricks Delta Shares from Snowflake; with Snowflake Artifact Repository”**  
📖 [Read the full blog here](https://medium.com/@phani.alapaty/4dbbc69ea435)

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
```

---

## ⚙️ Requirements

- Snowflake account with **Snowpark** and **External Access Integration** enabled  
- Databricks Delta Share config file (`config.share`)  
- Delta Sharing Python library (`delta-sharing==1.0.5`)  
- A Snowpark-compatible warehouse (e.g., x86 architecture)

---

## 🚀 Quick Start

### Clone the repo

```bash
git clone https://github.com/your-username/delta-sharing-snowflake.git
cd delta-sharing-snowflake
```

### Setup

- Review and run the SQL scripts in `snowflake/` to set up network access and stored procedures.
- Upload your `config.share` file to the defined stage.

### Run the stored procedure:

```sql
CALL TEST_DELTA_SHARE_CDC_TIMESTAMP(NULL);  -- Initial load
```

### For subsequent incremental loads, use the timestamp returned by the previous run:

```sql
CALL TEST_DELTA_SHARE_CDC_TIMESTAMP('2025-05-05T21:47:34.764Z');
```

---

## 🛡️ Security Notes

- Snowflake’s network rules explicitly control **egress traffic**.
- All connections are secured with **TLS 1.2+**.
- Data pulled via Delta Sharing is **physically copied** (not zero-copy).
