from dotenv import load_dotenv
from urllib.parse import quote
import os

load_dotenv(dotenv_path='postgress/config/postgress.env')

def build_db_connection(prefix: str):
    user = os.getenv(f"DB_{prefix}_USERNAME")
    password = quote(os.getenv(f"DB_{prefix}_PASSWORD"))
    host = os.getenv(f"DB_{prefix}_HOST")
    port = os.getenv(f"DB_{prefix}_PORT")
    name = os.getenv(f"DB_{prefix}_NAME")

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"