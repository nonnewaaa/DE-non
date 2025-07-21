from dotenv import load_dotenv
from urllib.parse import quote
import os

load_dotenv(dotenv_path='postgress.env')

user = os.getenv('DB_USERNAME')
password = quote(os.getenv('DB_PASSWORD'))
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')