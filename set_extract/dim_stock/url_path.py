import configparser
config = configparser.ConfigParser()
config.read('set_extract/url_path.conf')

dim_cfg = config.get("paths", "dim_cfg")
dim_url = config.get("urls", "dim_url")