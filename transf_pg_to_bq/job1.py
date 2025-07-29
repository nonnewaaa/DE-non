from decimal import Decimal
from postgress.fn_postgress import *
from bigquery.fn_bigquery import *

import configparser
config = configparser.ConfigParser()
config.read('transf_pg_to_bq/bq_cfg.conf')

def grep_config_by_task(task_name):
    bq_config = pd.DataFrame([{
        "project": config.get(task_name, "project"),
        "dataset": config.get(task_name, "dataset"),
        "table": config.get(task_name, "table")
    }])
    return bq_config

def migrate_postgres_to_bigquery(db_type, bq_config, query):
    df = sql_to_df(query, db_type)
    for col in df.select_dtypes(include='float').columns:
        df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
    upload_to_bigquery(df, bq_config)

if __name__ == "__main__":
    
    #dim_conf = grep_config_by_task("dim_stock_detail")
    #SQL_QUERY = "SELECT * FROM analytics.dim_stock_detail"
    #migrate_postgres_to_bigquery("Primary", dim_conf, SQL_QUERY)

    fin_conf = grep_config_by_task("raw_set_fin_data")
    SQL_QUERY = "SELECT * FROM analytics.raw_set_fin_data"
    migrate_postgres_to_bigquery("Primary", fin_conf, SQL_QUERY)
