from shippy.db.sqldb import ShippySQLite
from shippy.data_ingest import ingest_functions

if __name__ == '__main__':
    # Initialize database.
    db = ShippySQLite()
    # Ingest raw messages into the database.
    raw_messages = ingest_functions.raw_messages_csv_to_df()
    db.write_df_to_table(raw_messages, 'raw_messages', if_exists='replace')
    # Ingest weather stations information into the database.
    weather_stations = ingest_functions.weather_data_json_to_df()
    db.write_df_to_table(weather_stations, 'weather_stations', if_exists='replace')
    # Ingest normalized weather data into the database.
    weather_data = ingest_functions.weather_data_json_normalized_to_df()
    db.write_df_to_table(weather_data, 'weather_data', if_exists='replace')
    # Ingest raw messages with calculated nearest weather station coordinates into the database.
    raw_messages_stations = ingest_functions.raw_messages_and_nearest_station_to_df()
    db.write_df_to_table(raw_messages_stations, 'raw_messages_stations', if_exists='replace')