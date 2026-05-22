import pandas as pd
import datetime as dt

def transform_data(data):
    data['Date'] = pd.to_datetime(data['Date'])
    data['Month'] = data['Date'].dt.month
    data['CPI'] = data['CPI'].fillna(0)
    data['Weekly_Sales'] = data['Weekly_Sales'].fillna(0)
    data['Unemployment'] = data['Unemployment'].fillna(0)
    data = data.drop(columns=['Unnamed: 0','index','Date','Temperature','Fuel_Price','MarkDown1','MarkDown2','MarkDown3','MarkDown4','MarkDown5','Size','Type'])
    data = data[data['Weekly_Sales'] > 10000]
    return data

def avg_weekly_sales_per_month(data):
    agg_data = data.groupby(['Month']).agg({
        'Weekly_Sales':'mean'
    }).reset_index().round()
    agg_data = agg_data.rename(columns={'Weekly_Sales':'Avg_Sales'})
    return agg_data