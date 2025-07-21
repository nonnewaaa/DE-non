from postgress.conn_postgress import gen_connection_pg, sql_to_df

conn_str = gen_connection_pg("PRIMARY")
sql_query = """SELECT * FROM public.users_non"""
df = sql_to_df(sql_query, conn_str)
print(df.head())
