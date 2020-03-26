# =============================================================================
# Extract and prepare meteorological data from https://www.ncdc.noaa.gov/
# the National Centers for Environmental Information
# =============================================================================

from ftplib import FTP
import pandas as pd
#import geopandas as gpd

# Set to True to download the files from the source. This should be performed as rarely as
# possible. The scrpt downloads the files to local.
download_files = True

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
                           colspes=[(0, 2), (3, 50)],
                           header=None,
                           names=["COUNTRY_CODE", "COUNTRY"])

# Join country to df_stations
df_stations["COUNTRY_CODE"] = df_stations["ID"].apply(lambda x: x[:2])
df_stations = df_stations.merge(df_countries, on=["COUNTRY_CODE"], how="left")

# Countries we want to take into account
relevant_country_codes = ["FR"]

df_stations = df_stations[df_stations["COUNTRY_CODE"].isin(relevant_country_codes)]
