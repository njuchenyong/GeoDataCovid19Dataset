# =============================================================================
# Extract and prepare meteorological data from https://www.ncdc.noaa.gov/
# the National Centers for Environmental Information
# =============================================================================

from ftplib import FTP
import pandas as pd
import tarfile as tar
import numpy as np
#import geopandas as gpd

# Set to True to download the files from the source. This should be performed as rarely as
# possible. The scrpt downloads the files to local.
download_files = False

if download_files:
    ftp = FTP("ftp.ncdc.noaa.gov")
    print(ftp.login())
    
    ftp.cwd("/pub/data/ghcn/daily/")
    
    # readme file, contains useful information about the data
    with open("readme.txt", "wb") as fp:
        ftp.retrbinary('RETR readme.txt', fp.write)
    
    # Extract stations metadata.
    with open("stations_metadata.txt", "wb") as fp:
        ftp.retrbinary('RETR ghcnd-stations.txt', fp.write)
    
    # Country codes
    with open("country_codes.txt", "wb") as fp:
        ftp.retrbinary('RETR ghcnd-countries.txt', fp.write)
    
    # All daily data. This is 3gb+, takes a long time.
    with open("all_daily_data.tar.gz", "wb") as fp:
        ftp.retrbinary('RETR ghcnd_all.tar.gz', fp.write)
    with tar.open("all_daily_data.tar.gz") as tar_all:
        tar_all.extractall(path="./all_daily/")
        
df_stations = pd.read_fwf("stations_metadata.txt",
                          colspecs=[(0, 11),
                                    (12, 20),
                                    (21, 30),
                                    (31, 37),
                                    (38, 40),
                                    (41, 71),
                                    (72, 75),
                                    (76, 79),
                                    (80, 85)],
                          header=None,
                          names=["ID",
                                 "LATITUDE",
                                 "LONGITUDE",
                                 "ELEVATION",
                                 "STATE",
                                 "NAME",
                                 "GSN FLAG",
                                 "HCN/CRN FLAG",
                                 "WMO ID"]
                          )

df_countries = pd.read_fwf("country_codes.txt",
                           colspecs=[(0, 2), (3, 50)],
                           header=None,
                           names=["COUNTRY_CODE", "COUNTRY"])

# Join country to df_stations
df_stations["COUNTRY_CODE"] = df_stations["ID"].apply(lambda x: x[:2])
df_stations = df_stations.merge(df_countries, on=["COUNTRY_CODE"], how="left")

# Countries we want to take into account
relevant_country_codes = ["FR"]

df_stations = df_stations[df_stations["COUNTRY_CODE"].isin(relevant_country_codes)]

# Data from each of the considered stations
colnames = ["ID", "YEAR", "MONTH", "ELEMENT"]
colspecs = [(0, 11), (11, 15), (15, 17), (17, 21)]
last = 21
for day in range(1, 32):
    colnames.append(f"VALUE{day}")
    colspecs.append((last, last+5))
    last += 5
    
    colnames.append(f"MFLAG{day}")
    colspecs.append((last, last+1))
    last += 1
    
    colnames.append(f"QFLAG{day}")
    colspecs.append((last, last+1))
    last += 1
    
    colnames.append(f"SFLAG{day}")
    colspecs.append((last, last+1))
    last += 1
# function used to ut the data in correct shape
def reshape_daily_data(month_data):
    month_data = month_data.iloc[0]
    
    id_station = []
    date = []
    element = []
    value = []
    measurment_flag = []
    quality_flag = []
    source_flag = []
    
    for day in range(1, 32):
        id_station.append(month_data["ID"])
        date.append(f"{day}-{month_data['MONTH']}-{month_data['YEAR']}")
        element.append(month_data["ELEMENT"])
        value.append(month_data[f"VALUE{day}"])
        measurment_flag.append(month_data[f"MFLAG{day}"])
        quality_flag.append(month_data[f"QFLAG{day}"])
        source_flag.append(month_data[f"SFLAG{day}"])
    
    return pd.DataFrame(data={"ID": id_station,
                              "DATE": date,
                              "ELEMENT": element,
                              "VALUE": value,
                              "MEASURMENT_FLAG": measurment_flag,
                              "QUALITY_FLAG": quality_flag,
                              "SOURCE_FLAG": source_flag})

def time_series_of_station(station_name):
    df = pd.read_fwf(f"./all_daily/ghcnd_all/{station_name}.dly",
                     colspecs=colspecs,
                     header=None,
                     names=colnames)
    # Select data starting from November 2019
    df = df[np.logical_or(df["YEAR"]>=2020, np.logical_and(df["YEAR"]==2019, df["MONTH"]>=11))]
    if df.empty:
        return
    # Put data in a time series
    df = df.groupby(["ID", "YEAR", "MONTH", "ELEMENT"]).apply(reshape_daily_data)
    df.reset_index(drop=True, inplace=True)
    # join station metadata
    df = df.merge(df_stations, how="left", on=["ID"])
    return df

# final dataframe
df_daily_information = df_stations[["ID"]].groupby("ID").apply(lambda row: time_series_of_station(row.iloc[0]["ID"]))
df_daily_information.reset_index(drop=True, inplace=True)
