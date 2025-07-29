from bigquery.bq_config import bq_conf
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigquery/data-non-project-creadentials.json"

def upload_to_bigquery(df, df_conf):
    config = bq_conf(df_conf)
    client = bigquery.Client(project=config.project)
    table_id = f"{config.project}.{config.dataset}.{config.table}"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Uploaded {len(df)} rows to {table_id}")