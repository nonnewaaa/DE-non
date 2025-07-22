import configparser
config = configparser.ConfigParser()
config.read('set_extract/fin_stmt/url_path.conf')

set_list_path = config.get("paths", "set_list_path")
url = config.get("urls", "url")