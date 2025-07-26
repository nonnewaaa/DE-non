import configparser
config = configparser.ConfigParser()
config.read('set_extract/set_extract.conf')

dim_cfg = config.get("paths", "dim_cfg")
dim_url = config.get("urls", "dim_url")