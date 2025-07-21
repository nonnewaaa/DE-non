from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

def clean_format(df):
    header_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
    df_formatted = pd.concat([header_row, df], ignore_index=True)
    df_formatted.columns = [1,2,3,4,5,6]
    return df_formatted

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
url = "https://www.set.or.th/en/market/product/stock/quote/ADVANC/financial-statement/company-highlights"
driver.get(url)

html = driver.page_source
data_list = pd.read_html(StringIO(html))
df1 = data_list[0]
df2 = data_list[1]

df1 = df1[~df1["Period as of"].isin(["Financial Data", "Financial Ratio"])]

df1_formatted = clean_format(df1)
df2_formatted = clean_format(df2)
df_combined = pd.concat([df1_formatted, df2_formatted], ignore_index=True)

df_trans = df_combined.T
df_trans.columns = df_trans.iloc[0]
df_trans = df_trans.drop(index=1).reset_index(drop=True)

df_trans["Period as of"] = df_trans["Period as of"].apply(lambda x: " ".join(str(x).split()[-3:]))
date_cols = ["Statistics as of", "F/S Period (As of date)", "Period as of"]

for col in date_cols:
    df_trans[col] = pd.to_datetime(df_trans[col], format="%d %b %Y")

df_trans["Stock"] = "ADVANC"

print(df_trans)