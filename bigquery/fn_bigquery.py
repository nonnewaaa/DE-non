import pandas as pd
import configparser
from decimal import Decimal
from google.cloud import bigquery
from bigquery.bq_config import bq_conf

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigquery/data-non-project-creadentials.json"

config = configparser.ConfigParser()
config.read('bigquery/table.conf')

def grep_config_table(task_name):
    bq_config = pd.DataFrame([{
        "project": config.get(task_name, "project"),
        "dataset": config.get(task_name, "dataset"),
        "table": config.get(task_name, "table")
    }])
    return bq_config

def upload_to_bigquery(df, df_conf):
    config = bq_conf(df_conf)
    client = bigquery.Client(project=config.project)
    table_id = f"{config.project}.{config.dataset}.{config.table}"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Uploaded {len(df)} rows to {table_id}")

def df_to_bigquery(df, table_name):
    table_cfg = grep_config_table(table_name)
    for col in df.select_dtypes(include='float').columns:
        df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
    upload_to_bigquery(df, table_cfg)