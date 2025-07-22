import pandas as pd
import numpy as np

from postgress.config.get_config import *
from sqlalchemy import create_engine
from utils.exception import DataFrameException
from utils.fn_validate_data import *

def gen_connection_pg(db_name: str):
    conn_str = build_db_connection(db_name)
    return conn_str

def sql_to_df(sql_stmt: str, db_name: str) -> pd.DataFrame:
    conn_str = gen_connection_pg(db_name)
    engine = create_engine(conn_str)
    df = pd.read_sql(sql_stmt, engine)
    return df

def get_identity_col_db(table, db_name):
    query_identity = f"""SELECT OBJECT_NAME(object_id) AS TableName, name AS ColumnName 
                         FROM sys.identity_columns 
                         WHERE OBJECT_NAME(object_id) = '{table}';"""
    
    df_identity = sql_to_df(query_identity, db_name)
    col_identity = df_identity["ColumnName"].tolist()
    return col_identity

def null_columns_check(df, table, db_name): # check not null columns from db to prevent insert df null in not null columns 
    query = f""" SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE 
                 FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = '{table}'; """
    df_query = sql_to_df(query, db_name)
    df_query = df_query[df_query["IS_NULLABLE"] == "NO"]
    not_null_columns = df_query["COLUMN_NAME"]
    identity_col = get_identity_col_db(table, db_name)
    not_null_columns = [col for col in not_null_columns if col not in identity_col]

    if df[not_null_columns].isnull().any().any():
        null_columns = df[not_null_columns].columns[df[not_null_columns].isnull().any()].tolist()
        raise DataFrameException(f"Column {null_columns} contains null values and unable to insert or update to not null columns [Table : {table}]."
                                 , df[df[not_null_columns].isnull().any(axis=1)])

#------------------------------ Insert - Update - Delete -------------------------------
def insert_to_table(table_name, df, db_name, null_check=None):
    if not df.empty:
        date_cols = df.select_dtypes(include=["datetime64"]).columns
        df[date_cols] = df[date_cols].apply(lambda col: col.dt.strftime('%Y-%m-%d'))
        if null_check == None:
            print(f"Inserting to {table_name} ...")
            null_columns_check(df, table_name, db_name)
            #if not update_col_keys is None:
            #    check_dup_pri_key(df, update_col_keys)

        df = df.apply(lambda x: np.where(x.isna(), None, x))
        insert_stmt = f"""
            INSERT INTO {table_name}
                ({", ".join(df.columns)})
            VALUES
                ({", ".join(["?"] * len(df.columns))})
        """
        try:
            conn_str = gen_connection_pg(db_name)
            engine = create_engine(conn_str)
            engine.execute(insert_stmt, df.values.tolist())
        except Exception as e:
            print(e)
        print(f"Insert to table {table_name} done.")