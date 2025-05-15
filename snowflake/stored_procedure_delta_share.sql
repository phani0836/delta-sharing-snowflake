CREATE OR REPLACE PROCEDURE TEST_DELTA_SHARE_CDC_TIMESTAMP(STARTING_TIMESTAMP VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION='3.9'
RESOURCE_CONSTRAINT=(architecture='x86')
HANDLER='main'
EXTERNAL_ACCESS_INTEGRATIONS=(dbx_ext_access_integration)
PACKAGES=('snowflake-snowpark-python')
ARTIFACT_REPOSITORY=snowflake.snowpark.pypi_shared_repository
ARTIFACT_REPOSITORY_PACKAGES=('delta-sharing==1.0.5')
AS
$$
import delta_sharing, os, traceback, pandas as pd
from datetime import datetime
from snowflake.snowpark import Session
from requests.exceptions import HTTPError
from snowflake.snowpark.functions import when_matched, when_not_matched


def _fmt(ts: str) -> str:
    if not ts:
        return ts

    # if it’s all digits, assume epoch‐millis
    if ts.isdigit():
        ms = int(ts)
        dt = datetime.utcfromtimestamp(ms / 1000.0)
    else:
        # ISO8601 w/ optional Z
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))

    # format to milliseconds + ‘Z’
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'

def main(session: Session, starting_timestamp_str: str) -> str:
    target = "HOL_ICE_DB.PUBLIC.DELTA_SHARE_CUSTOMERS"
    # ensure target table in snowflake exists
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {target} (
        customer_id NUMBER,
        name        VARCHAR,
        region      VARCHAR
      );
    """).collect()

    # pull down the share profile
    stage = "@HOL_ICE_DB.PUBLIC.DELTA_SHARE_STAGE/config.share"
    session.file.get(stage, "/tmp")
    profile = "/tmp/config.share"
    uri     = f"{profile}#demo_delta_share.default.customers_new"
    client  = delta_sharing.SharingClient(profile)

    # Build timestamps
    start_ts = None
    if starting_timestamp_str and starting_timestamp_str.lower() != 'none':
        start_ts = _fmt(starting_timestamp_str)
    end_raw = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
    end_ts  = _fmt(end_raw)

    # load the data
    try:
        if not start_ts:
            # ─── INITIAL LOAD ─────────────────────────────────────────────────
            pdf = delta_sharing.load_as_pandas(uri)

            # if the snapshot comes back empty, bail
            if pdf.empty:
                return f"Initial load found 0 rows."

            # lowercase the columns so we know we have customer_id, etc.
            pdf.columns = [c.lower() for c in pdf.columns]

            # write all rows in one shot
            session.create_dataframe(pdf) \
                   .write.mode("overwrite") \
                   .save_as_table(target)

            return f"Initial load: {len(pdf)} rows. Next start: {end_ts}"

        else:
            # ─── INCREMENTAL LOAD ────────────────────────────────────────────────
            # first try with explicit ending timestamp
            try:
                pdf = delta_sharing.load_table_changes_as_pandas(
                    uri,
                    starting_timestamp=start_ts,
                    ending_timestamp=end_ts
                )
            except HTTPError:
                # on HTTP errors, retry without ending_timestamp
                pdf = delta_sharing.load_table_changes_as_pandas(
                    uri,
                    starting_timestamp=start_ts
                )

            if pdf.empty:
                return f"No new changes since {start_ts}. Next start still: {end_ts}"
            else:
                temp_table_name='TEMP_DELTA_SHARE_CUSTOMERS'
                session.write_pandas(pdf,temp_table_name, auto_create_table=True, schema='PUBLIC')
            
            # do the merge (inserts + upserts + deletes)
            merge_sql = f"""
            MERGE INTO {target} AS T
            USING (
              SELECT
                "customer_id",
                "name",
                "region",
                "_change_type"
              FROM (
                SELECT
                  *,
                  ROW_NUMBER() OVER (
                    PARTITION BY "customer_id"
                    ORDER BY "_commit_timestamp" DESC
                  ) AS rn
                FROM HOL_ICE_DB.PUBLIC.TEMP_DELTA_SHARE_CUSTOMERS
                WHERE "_change_type" <> 'update_preimage'
              )
              WHERE rn = 1
            ) AS S
              ON T."customer_id" = S."customer_id"
            
            WHEN MATCHED AND S."_change_type" IN ('delete','update_preimage') THEN
              DELETE
            
            WHEN MATCHED AND S."_change_type" = 'update_postimage' THEN
              UPDATE SET
                T."name"   = S."name",
                T."region" = S."region"
            
            WHEN NOT MATCHED AND S."_change_type" = 'insert' THEN
              INSERT (
                "customer_id",
                "name",
                "region"
              )
              VALUES (
                S."customer_id",
                S."name",
                S."region"
              )
            ;
            """
            session.sql(merge_sql).collect()

            #**drop the temp staging table on sucessful completion**
            session.sql("DROP TABLE IF EXISTS PUBLIC.TEMP_DELTA_SHARE_CUSTOMERS").collect()
            
            next_start = _fmt(str(pdf['_commit_timestamp'].max()))
            return f"Loaded {len(pdf)} changes. Next start: {next_start}"

    except Exception as e:
        return f"Error running load: {e}\\n{traceback.format_exc()}"
$$;