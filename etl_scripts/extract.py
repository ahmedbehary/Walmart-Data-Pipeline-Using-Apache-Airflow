import pandas as pd

def extract_data(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    store_data_df = pd.read_csv(store_data)
    merged_df = store_data_df.merge(extra_df, on = "index")
    return merged_df
