from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO

import time
import pandas as pd
import numpy as np
from postgress.fn_postgress import insert_to_table
from url_path import *

class Config:
    def __init__(self, df_conf):
        self.stock_list = df_conf.loc[0, "stock_list"].split("|")
        self.df_columns = df_conf.loc[0, "columns"].split("|")
        self.db_name = df_conf.loc[0, "db_name"]
        self.schema = df_conf.loc[0, "schema"]
        self.table_name = df_conf.loc[0, "table_name"]

def clean_df(df, col_target):
    df = df.drop(columns=["Factsheet (Click to clear sorting)"])
    df = df.replace("-", None)
    df.columns = col_target
    return df

if __name__ == "__main__":
    df_conf = pd.read_csv(hist_cfg)
    config = Config(df_conf)
    main_df = pd.DataFrame()

    for stock in config.stock_list:
        df = pd.DataFrame()
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        url = hist_url.replace("{stock_symbol}", stock)
        driver.get(url)

        while True:
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                tables = pd.read_html(StringIO(driver.page_source))
                current_df = tables[1] 
                df = pd.concat([df, current_df], ignore_index=True)

                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Go to next page']"))
                )

                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(0.5)
                next_button.click()

            except Exception as e:
                print("No more pages or error:", e)
                break

        driver.quit()
        df["stock_name"] = stock
        main_df = pd.concat([main_df, df], ignore_index=True)
        print("Columns:", main_df.columns.tolist())
        print("First row:", main_df.iloc[0].to_dict())

    #df = clean_df(df, config.df_columns)
    #insert_to_table(config.table_name, df, config.schema, config.db_name)