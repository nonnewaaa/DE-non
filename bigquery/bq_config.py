class bq_conf:
    def __init__(self, df_conf):
        self.project = df_conf.loc[0, "project"]
        self.dataset = df_conf.loc[0, "dataset"]
        self.table = df_conf.loc[0, "table"]