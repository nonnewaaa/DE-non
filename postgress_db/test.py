import pandas as pd
from get_config import *
from sqlalchemy import create_engine

conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(conn_str)

df = pd.read_sql('SELECT * FROM public.users_non', engine)
print(df.head())

