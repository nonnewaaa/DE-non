from utils.exception import DataFrameException

def check_dup_pri_key(df, col_check):
    duplicates = df[df.duplicated(subset=col_check, keep=False)]
    if not duplicates.empty:
        raise DataFrameException(f"Duplicate primary keys found while insert/update : {col_check}", duplicates)