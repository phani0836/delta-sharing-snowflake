CREATE or replace NETWORK RULE external_access_rule_dbx
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('nvirginia.cloud.databricks.com:443','<bucket_name>');