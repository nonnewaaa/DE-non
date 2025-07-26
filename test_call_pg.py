from postgress.fn_postgress import gen_connection_pg, sql_to_df

sql_query = """SELECT * FROM public.users_non"""
df = sql_to_df(sql_query, "PRIMARY")
print(df.head())
