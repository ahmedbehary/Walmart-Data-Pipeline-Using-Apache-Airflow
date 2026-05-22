import pandas as pd
import os

def load_data(full_data,full_data_path,agg_data,agg_data_path):
    full_data.to_csv(full_data_path,index=False)
    agg_data.to_csv(agg_data_path,index=False)

def validation(file_path):
    existence = os.path.exists(file_path)
    print(existence)
