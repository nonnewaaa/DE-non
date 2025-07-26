import configparser
config = configparser.ConfigParser()
config.read('set_extract/set_extract.conf')

set_list_path = config.get("paths", "fin_cfg")
url = config.get("urls", "fin_url")