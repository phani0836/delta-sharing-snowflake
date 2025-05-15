CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbx_ext_access_integration
  ALLOWED_NETWORK_RULES = (external_access_rule_dbx)
  ENABLED = true;