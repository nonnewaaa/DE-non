import configparser
config = configparser.ConfigParser()
config.read('set_extract/set_extract.conf')

hist_cfg = config.get("paths", "hist_cfg")
hist_url = config.get("urls", "hist_url")