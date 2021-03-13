import json
from math import radians, cos, sin, asin, sqrt

import pandas as pd
import numpy as np


def _parse_datetime(datetime_column):
    """
    Private function to correctly parse datetime columns.
    """
    return pd.to_datetime(datetime_column, unit='s')


def raw_messages_csv_to_df(csv: str = 'data/raw_messages.csv',
                           clean_data: bool = True) -> pd.DataFrame:
    """Read raw messages data csv file into a pandas dataframe.

    Args:
        csv: Name of the csv file to read, default 'data/raw_messages.csv'.
        clean_data: Performs deduplication and raw message noise cleanup if set to True.

    Returns:
        Raw messages data as pandas dataframe.

    """
    # Read raw messages, constructing a MultiIndex from device_id and correctly parsed datetime.
    raw_messages = pd.read_csv(csv,
                               index_col=['device_id', 'datetime'],
                               parse_dates=['datetime'],
                               date_parser=_parse_datetime).sort_index()
    # Perform cleaning if it is asked so.
    if clean_data:
        # Drop duplicates.
        raw_messages.drop_duplicates(inplace=True)
        # Remove non-alphanumeric noisy characters from raw_message column.
        raw_messages['raw_message'] = raw_messages['raw_message'].str.replace('[^a-zA-Z0-9,.]', '', regex=True)
    # Expand raw message column into separate columns.
    raw_messages = pd.concat([raw_messages.drop('raw_message', axis=1),
                              raw_messages['raw_message'].str.split(',', expand=True)],
                             axis=1)
    # Rename the expanded columns into correct raw message attributes.
    rename_dict = {0: 'status',
                   1: 'lat',
                   2: 'lat_dir',
                   3: 'lon',
                   4: 'lon_dir',
                   5: 'spd_over_grnd',
                   6: 'true_course',
                   7: 'datestamp',
                   8: 'mag_variation',
                   9: 'mag_var_dir'}
    raw_messages.rename(columns=rename_dict, inplace=True)
    # Fix dtypes of numeric columns and return the result.
    dtypes_dict = {'lat': np.float64,
                   'lon': np.float64,
                   'spd_over_grnd': np.float64,
                   'true_course': np.float64,
                   'datestamp': np.int64,
                   'mag_variation': np.float64}

    return raw_messages.astype(dtypes_dict, errors='raise').round(decimals=3)


def weather_data_json_to_df(json_file: str = 'data/weather_data.json') -> pd.DataFrame:
    """Read weather data json file into a pandas dataframe.

    Args:
        json_file: Name of the json file to read, default data/weather_data.json'.

    Returns:
        Weather data as pandas dataframe.

    """
    # Read weather_data.json at weather stations level (without normalizing data field).
    weather_stations = pd.read_json(json_file).rename(columns={'lat': 's_lat',
                                                               'lon': 's_lon'})
    # Fix dtypes and return the result.
    dtypes_dict = {'data': str,
                   'sources': str}

    return weather_stations.astype(dtypes_dict, errors='raise').round(decimals=3)


def weather_data_json_normalized_to_df(json_file: str = 'data/weather_data.json') -> pd.DataFrame:
    """Read weather data json file into a pandas dataframe, with data records normalized into separate columns.

    Args:
        json_file: Name of the json file to read, default data/weather_data.json'.

    Returns:
        Weather data as pandas dataframe, with normalized data records.

    """
    # Read full weather data by normalizing data field into separate columns, setting 'timestamp_utc' as datetime index.
    with open(json_file) as f:
        weather_data_json = json.load(f)

    weather_data = pd.json_normalize(weather_data_json, record_path='data',
                                     meta=['station_id', 'lat', 'lon']).rename(columns={'lat': 's_lat',
                                                                                        'lon': 's_lon',
                                                                                        'datetime': 's_datetime'})
    weather_data['timestamp_utc'] = pd.to_datetime(weather_data['timestamp_utc'])
    weather_data = weather_data.set_index('timestamp_utc').sort_index()
    # Fix dtypes of numeric columns and return the result
    dtypes_dict = {'s_lat': np.float64,
                   's_lon': np.float64}

    return weather_data.astype(dtypes_dict, errors='raise').round(decimals=3)


def raw_messages_and_nearest_station_to_df(csv: str = 'data/raw_messages.csv',
                                           json_file: str = 'data/weather_data.json') -> pd.DataFrame:
    # Read raw messages and weather stations data.
    raw_messages_stations = raw_messages_csv_to_df(csv)
    weather_stations = weather_data_json_to_df(json_file)

    # Function for applying on raw data to find nearest weather station for each data point.
    def find_nearest_station(lat: float,
                             long: float):
        distances = weather_stations.apply(lambda row: _dist(lat, long, row['s_lat'], row['s_lon']),
                                           axis=1)
        return weather_stations.loc[distances.idxmin(), ['s_lat', 's_lon']]

    # Find nearest weather station for each data point and add its coordinates to raw data.
    raw_messages_stations[['s_lat', 's_lon']] = raw_messages_stations.apply(lambda row: find_nearest_station(row['lat'],
                                                                                                             row[
                                                                                                                 'lon']),
                                                                            axis=1)

    return raw_messages_stations


# Steal haversine formula implementation from internet.
# https://medium.com/analytics-vidhya/finding-nearest-pair-of-latitude-and-longitude-match-using-python
def _dist(lat1: float,
          long1: float,
          lat2: float,
          long2: float) -> float:
    """
    Replicating the same formula as mentioned in Wiki
    """
    # convert decimal degrees to radians
    lat1, long1, lat2, long2 = map(radians, [lat1, long1, lat2, long2])
    # haversine formula
    dlon = long2 - long1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371 * c
    return km
