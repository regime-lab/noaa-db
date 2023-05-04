import xarray as xr
import pandas as pd

# Parquet NOAA interface to stdmet (Standard Meteorological Data)
def parq_noaa_data(buoy_ids, start_date, end_date):
  
    dataframes = []
    for buoy_id in buoy_ids:
      
        # Get each buoys data into a sub-frame
        ds = xr.open_dataset(f'https://dods.ndbc.noaa.gov/thredds/dodsC/data/stdmet/{buoy_id}/{buoy_id}.ncml')
        ds = ds.sel(time=slice(start_date, end_date))
        df = ds.to_dataframe().reset_index()
        df['buoy_id'] = buoy_id
        dataframes.append(df)
        
    # Concat sub-frames and save to parquet file 
    noaa_df = pd.concat(dataframes)  
    noaa_df.to_parquet('noaa_data.parquet')
    
    return noaa_df
