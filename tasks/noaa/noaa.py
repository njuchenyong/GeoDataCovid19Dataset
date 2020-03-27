"""Extract and prepare meteorological data from https://www.ncdc.noaa.gov/
the National Centers for Environmental Information
"""

import logging
import tarfile as tar
from ftplib import FTP

import pandas as pd

from references import load_dataset

logging.basicConfig(level=logging.DEBUG)

NOAA_FTP_FILES = {
    'readme.txt': 'RETR readme.txt',
    "stations_metadata.txt": 'RETR ghcnd-stations.txt',
    'country_codes.txt': 'RETR ghcnd-countries.txt',
    'all_daily_data.tar.gz': 'RETR ghcnd_all.tar.gz'
}


def download_noaa_files():
    logging.info('Connecting to NOAA FTP server.')
    ftp = FTP("ftp.ncdc.noaa.gov")
    print(ftp.login())

    ftp.cwd("/pub/data/ghcn/daily/")

    for filename, action in NOAA_FTP_FILES:
        logging.debug('Downloading %s', filename)
        if filename.endswith('tar.gz'):
            logging.debug('This is file is more than 3Gb+, it may take a long time.')
        with open(filename, "wb") as fp:
            ftp.retrbinary(action, fp.write)

    logging.debug('Extracting daily data.')
    with tar.open("all_daily_data.tar.gz") as tar_all:
        tar_all.extractall(path="./all_daily/")

    logging.debug('Done!')


def reshape_daily_data(month_data):
    """Reshape a row of monthly data into a dataframe with date as a column."""
    month_data = month_data.iloc[0]

    station_id = month_data["ID"]
    year = month_data['YEAR']
    month = month_data['MONTH']
    element = month_data['ELEMENT']

    data = [{
        "ID": station_id,
        "DATE": f'{day}-{month}-{year}',
        "ELEMENT": element,
        "VALUE": month_data[f'VALUE{day}'],
        "MEASURMENT_FLAG": month_data[f'MFLAG{day}'],
        "QUALITY_FLAG": month_data[f'QFLAG{day}'],
        "SOURCE_FLAG": month_data[f'SFLAG{day}'],
    } for day in range(1, 32)]

    return pd.DataFrame(data)


def time_series_of_station(station_row):
    station_name = station_row.iloc[0]['ID']
    df = load_dataset(station_name)
    # Select data starting from November 2019
    df = df[(df.YEAR >= 2020) | ((df.YEAR == 2019) & (df.MONTH >= 11))]

    if df.empty:
        return

    # Put data in a time series
    df = df.groupby(["ID", "YEAR", "MONTH", "ELEMENT"]).apply(reshape_daily_data)
    return df.reset_index(drop=True)


def process_noaa_data(countries):
    """Returns a dataset for the given countries """

    df_stations = load_dataset('stations')
    df_countries = load_dataset('countries')

    # Join country to df_stations
    df_stations["COUNTRY_CODE"] = df_stations["ID"].str.slice(0, 2)
    df_stations = df_stations.merge(df_countries, on=["COUNTRY_CODE"], how="left")

    df_stations = df_stations[df_stations["COUNTRY_CODE"].isin(countries)]

    df_daily_information = df_stations[['ID']].groupby("ID").apply(time_series_of_station)
    df_daily_information.reset_index(drop=True, inplace=True)

    return df_daily_information.merge(df_stations, how="left", on=["ID"])

