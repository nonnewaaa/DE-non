from postgress.fn_postgress import *
from bigquery.fn_bigquery import *

import configparser
config = configparser.ConfigParser()
config.read('transf_pg_to_bq/bq_cfg.conf')

bq_config = pd.DataFrame([{
    "project": config.get("job1", "project"),
    "dataset": config.get("job1", "dataset"),
    "table": config.get("job1", "table")
}])

def migrate_postgres_to_bigquery(db_type, bq_config, query):
    df = sql_to_df(query, db_type)
    upload_to_bigquery(df, bq_config)

if __name__ == "__main__":
    #main
    SQL_QUERY = "SELECT * FROM analytics.dim_stock_detail"
    migrate_postgres_to_bigquery("Primary", bq_config, SQL_QUERY)
