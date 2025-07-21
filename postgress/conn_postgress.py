import pandas as pd
from postgress.config.get_config import *
from sqlalchemy import create_engine

def gen_connection_pg(db_type: str):
    conn_str = build_db_connection(db_type)
    return conn_str

def sql_to_df(sql_stmt: str, conn_str: str) -> pd.DataFrame:
    engine = create_engine(conn_str)
    df = pd.read_sql(sql_stmt, engine)
    return df