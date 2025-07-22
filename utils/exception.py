class DataFrameException(Exception):
    def __init__(self, message, df):
        self.df_str = df.to_string(index=False) if not df.empty else "Empty DataFrame"
        self.message = f"{message}\nDataFrame contents: \n{self.df_str}"
        super().__init__(self.message)