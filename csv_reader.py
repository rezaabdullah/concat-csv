# -*- coding: utf-8 -*-
"""
Author: Abdullah Reza

"""

import numpy as np
import pandas as pd
import datetime as dt
import dask.dataframe as dd
import geopandas
from shapely.geometry import Point
import glob
import csv

csv_files = glob.glob("*.csv")

cols_to_keep = [0, 2, 3, 5]
header_name = ["idfa", "lat", "lon", "timestamp"]
start_time = np.datetime64("2019-01-01 00:00:00")
end_time = np.datetime64("2019-01-07 11:59:59")

# Concatanate all the CSVs
def concat_csv(out_csv):
    df_list = []
    for file in csv_files:
        temp_df = pd.read_csv(file, header = None, usecols = cols_to_keep,
                              skiprows = 1, nrows = 1)
        file_date = pd.Series(pd.to_datetime(temp_df[5], unit = "s")).values
        if((file_date[0] > start_time) & (file_date[0] < end_time)):
            chunk_csv = pd.read_csv(file, header = None, usecols = cols_to_keep,
                skiprows = 1, chunksize = 10 ** 6)
            each_csv = pd.concat([chunk for chunk in chunk_csv])
            df_list.append(each_csv)
    merge_csv = pd.concat([df for df in df_list])
    merge_csv.to_csv(out_csv, header = header_name, index = False)
    return merge_csv
    
if __name__ == "__main__":
    df = concat_csv("test.csv")
    print(df.shape())