from ftplib import FTP
import pandas as pd

ftp = FTP("ftp.ncdc.noaa.gov")
print(ftp.login())

ftp.cwd("/pub/data/ghcn/daily/")

with open("stations_metadata.txt", "wb") as fp:
    ftp.retrbinary('RETR ghcnd-stations.txt', fp.write)

df_stations = pd.read_fwf("stations_metadata.txt",
                          colspecs=[(1, 12),
                                    (13, 21),
                                    (22, 31),
                                    (32, 38),
                                    (39, 41),
                                    (42, 72),
                                    (73, 76),
                                    (77, 80),
                                    (81, 86)],
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
