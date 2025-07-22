import pandas as pd
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from postgress.conn_postgress import *

from url_path import *

class Config:
    def __init__(self, df_conf):
        self.stock_list = df_conf.loc[0, "stock"].split("|")
        self.df_columns = df_conf.loc[0, "columns"].split("|")
        self.db_name = df_conf.loc[0, "db_name"]
        self.schema = df_conf.loc[0, "schema"]
        self.table_name = df_conf.loc[0, "table_name"]

def get_data_html(stock, url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    url = url.replace("{stock_symbol}", stock)
    print(f"Reading url from :{url} ...")
    driver.get(url)
    html = driver.page_source
    return html
    
def demote_header(df):
    header_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
    df_formatted = pd.concat([header_row, df], ignore_index=True)
    return df_formatted

def combine_df(df1, df2):    
    if df1.shape[1] == df2.shape[1]:
        column_names = list(range(1, len(df1.columns) + 1))
        df1.columns = column_names
        df2.columns = column_names

    else:
        most_col = max(df1.shape[1],df2.shape[1])
        column_names = list(range(1, most_col + 1))
        diff = df1.shape[1] - df2.shape[1]
        
        if diff > 0:
            df1.columns = column_names
            for i in range(diff):
                df2.insert(1, f'diff{i}', None)
            df2.columns = column_names

        else:
            df2.columns = column_names
            for i in range(diff):
                df1.insert(1, f'diff{i}', None)
            df1.columns = column_names
            
    df_combined = pd.concat([df1, df2], ignore_index=True)
    return df_combined

def clean_format(df):
    df_trans = df.replace("-", None)
    df_trans = df_trans.replace({pd.NaT: None, np.nan: None})

    df_trans = df_trans.T
    df_trans.columns = df_trans.iloc[0]
    df_trans = df_trans.drop(index=1).reset_index(drop=True)

    df_trans["Period as of"] = df_trans["Period as of"].apply(lambda x: " ".join(str(x).split()[:5]))  # --> Y/E 2021 31 Dec 2021
    df_trans["Period as of"] = df_trans["Period as of"].apply(lambda x: " ".join(str(x).split()[-3:])) # --> 31 Dec 2021
    date_cols = ["Statistics as of", "F/S Period (As of date)", "Period as of"] 

    for col in date_cols:
        df_trans[col] = pd.to_datetime(df_trans[col], format="%d %b %Y", errors='coerce') # 31 Dec 2021 --> 2021-12-31
        df_trans[col] = df_trans[col].astype(str).replace("NaT", None)

    df_trans.columns = config.df_columns
    return df_trans

def df_html_insert(html, stock_symbol):
    data_list = pd.read_html(StringIO(html))
    df1 = data_list[0]
    df2 = data_list[1]

    df1 = df1[~df1["Period as of"].isin(["Financial Data", "Financial Ratio"])]

    df1_formatted = demote_header(df1)
    df2_formatted = demote_header(df2)
    df_combined = combine_df(df1_formatted, df2_formatted)
    df = clean_format(df_combined)
    df["stock"] = stock_symbol

    print(df)
    insert_to_table(config.table_name, df, config.schema, config.db_name)

if __name__ == "__main__":
    df_conf = pd.read_csv(set_list_path)
    config = Config(df_conf)
    for symbol in config.stock_list:
        print(f"Current stock : {symbol} ...")
        html = get_data_html(symbol, url)
        df_html_insert(html, symbol)
        print(f"Insert complete [stock = {symbol}].")