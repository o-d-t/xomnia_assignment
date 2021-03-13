import pandas as pd
import numpy as np
import plotly.graph_objects as go


def _merge_raw_messages_weather(raw_messages_stations: pd.DataFrame,
                                weather_data: pd.DataFrame,
                                device_id: str,
                                date: np.datetime64) -> pd.DataFrame:
    """ Private function to perform merge of raw messages with nearest station coordinates and weather data.

    Args:
        raw_messages_stations: Raw messages with nearest station coordinates precalculated.
        weather_data: Normalized weather data.
        device_id: Ship to perform the merge for.
        date: Date to perform the merge for.

    Returns:
        Raw messages and weather data merged as pandas dataframe

    """
    # Merge asof on datetime indexes of raw data and weather data (nearest).
    # Merge by exact match of station lat and lon, so that data of the nearest station is grabbed.
    # Result will be a dataframe with weather data columns added on raw data.
    raw_and_weather = pd.merge_asof(
        raw_messages_stations.xs(device_id, level='device_id').loc[date:date + pd.DateOffset(1)],
        weather_data,
        left_index=True,
        right_index=True, by=['s_lat', 's_lon'],
        direction='nearest',
        tolerance=pd.Timedelta('1H'))
    return pd.concat({device_id: raw_and_weather}, names=['device_id'])


def num_ships_in_data(raw_messages: pd.DataFrame) -> int:
    """For how many ships do we have available data?

    Args:
        raw_messages: Data to identify number of the ships.

    Returns:
        Number of ships in the input data.

    """
    return raw_messages.index.get_level_values('device_id').unique().size


def avg_speeds_for_date(raw_messages: pd.DataFrame,
                        date: np.datetime64 = pd.to_datetime('2019-02-13')) -> pd.DataFrame:
    """Avg speed for all available ships for each hour of the date 2019-02-13.

    Args:
        raw_messages: Data to calculate average speeds.
        date: Date to calculate the average for, default 2019-02-13.

    Returns:

    """
    # Get speed info from data and calculate hourly average.
    speeds = raw_messages.loc[pd.IndexSlice[:, date:date + pd.DateOffset(1)], :]['spd_over_grnd']
    return speeds.groupby([pd.Grouper(level='device_id'),
                           pd.Grouper(level='datetime', freq='1H')]).mean().to_frame()


def wind_speed_max_min_for_ship(raw_messages_stations: pd.DataFrame,
                                weather_data: pd.DataFrame,
                                device_id: str = 'st-1a2090',
                                date: np.datetime64 = pd.to_datetime('2019-02-13')) -> pd.DataFrame:
    """Max & min wind speed for every available day for ship ”st-1a2090” only.

    Args:
        raw_messages_stations: Raw messages with nearest station coordinates precalculated.
        weather_data: Normalized weather data.
        device_id: Ship to calculate max&min wind speed for, default 'st-1a2090'.
        date: Date to calculate max&min wind speed for, default 2019-02-13.

    Returns:

    """
    # Get wind speeds from data.
    result = pd.DataFrame()
    raw_and_weather = _merge_raw_messages_weather(raw_messages_stations, weather_data, device_id, date)
    wind_speeds = raw_and_weather.loc[pd.IndexSlice[device_id, date:date + pd.DateOffset(1)], :]['wind_spd']
    # Calculate max&min speed and return the result.
    result['max_wind_speed'] = wind_speeds.groupby([pd.Grouper(level='device_id'),
                                                    pd.Grouper(level='datetime', freq='1H')]).max()
    result['min_wind_speed'] = wind_speeds.groupby([pd.Grouper(level='device_id'),
                                                    pd.Grouper(level='datetime', freq='1H')]).min()

    return result


def full_weather_visual_for_ship(raw_messages_stations: pd.DataFrame,
                                 weather_data: pd.DataFrame,
                                 device_id: str = 'st-1a2090',
                                 date: np.datetime64 = pd.to_datetime('2019-02-13')) -> go.Figure:
    """A way to visualize full weather conditions (example fields: general description, temperature, wind
    speed) across route of the ship ”st-1a2090” for date 2019-02-13.

    Args:
        raw_messages_stations: Raw messages with nearest station coordinates precalculated.
        weather_data: Normalized weather data.
        device_id: Ship to calculate max&min wind speed for, default 'st-1a2090'.
        date: Date to calculate max&min wind speed for, default 2019-02-13.

    Returns:
        Visualization of full weather conditions as Plotly  object.

    """
    # Get full weather information from data.
    raw_and_weather = _merge_raw_messages_weather(raw_messages_stations, weather_data, device_id, date)
    plot_data = raw_and_weather.loc[pd.IndexSlice[device_id, date:date + pd.DateOffset(1)], :]
    plot_data['weather_text'] = 'Timestamp: ' + plot_data.index.get_level_values(level='datetime').astype(str) + ', ' \
                                + 'Description: ' + plot_data['weather.description'].astype(str) + ', ' \
                                + 'Temperature: ' + plot_data['temp'].astype(str) + ', ' \
                                + 'Wind Speed: ' + plot_data['wind_spd'].astype(str)
    plot_data.reset_index(drop=True, inplace=True)

    # Build a plot object from full weather information and return the result.
    fig = go.Figure(data=go.Scattergeo(lon=plot_data['lon'],
                                       lat=plot_data['lat'],
                                       text=plot_data['weather_text'],
                                       mode='lines',
                                       line=dict(color='red')))

    fig.update_geos(fitbounds='locations',
                    resolution=50,
                    showocean=True, oceancolor="LightBlue",
                    showlakes=True, lakecolor="Blue",
                    showrivers=True, rivercolor="Blue")
    fig.update_layout(title=f"Route for {device_id} on {date}" + '<br>(Hover for weather conditions)',
                      geo_scope='europe')

    return fig
