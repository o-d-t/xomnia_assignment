# [START gae_python38_render_template]
# [START gae_python3_render_template]
from shippy.db.sqldb import ShippySQLite
from shippy.kpi_calculators import calculator_functions
from flask import Flask, jsonify, render_template

app = Flask(__name__)

# Read data from database into application instance.
db = ShippySQLite()
raw_messages_stations = db.read_table_to_df('raw_messages_stations',
                                            parse_dates=['datetime']).set_index(['device_id', 'datetime'])
weather_data = db.read_table_to_df('weather_data',
                                   parse_dates=['timestamp_utc']).set_index('timestamp_utc')

@app.route('/')
def root():
    num_ships_in_data = calculator_functions.num_ships_in_data(raw_messages_stations)
    avg_speeds_for_date = calculator_functions.avg_speeds_for_date(raw_messages_stations).to_html()
    wind_speed_max_min = calculator_functions.wind_speed_max_min_for_ship(raw_messages_stations,
                                                                          weather_data).to_html()
    full_weather_visual = calculator_functions.full_weather_visual_for_ship(raw_messages_stations,
                                                                            weather_data).to_html(full_html=False)
    return render_template('index.html',
                           num_ships_in_data=num_ships_in_data,
                           avg_speeds_for_date=avg_speeds_for_date,
                           wind_speed_max_min=wind_speed_max_min,
                           full_weather_visual=full_weather_visual)


@app.route('/num_ships_in_data', methods=["GET"])
def get_num_ships_in_data():
    num_ships_in_data = calculator_functions.num_ships_in_data(raw_messages_stations)
    return jsonify(f"{{\"number of ships in data\": {num_ships_in_data}}}")


@app.route('/avg_speeds_for_date', methods=["GET"])
def get_avg_speeds_for_date():
    avg_speeds_for_date = calculator_functions.avg_speeds_for_date(raw_messages_stations)
    return jsonify(avg_speeds_for_date.to_json(orient='index'))


@app.route('/wind_speed_max_min', methods=["GET"])
def get_wind_speed_max_min():
    wind_speed_max_min = calculator_functions.wind_speed_max_min_for_ship(raw_messages_stations,
                                                                          weather_data)
    return jsonify(wind_speed_max_min.to_json(orient='index'))


@app.route('/full_weather_visual', methods=["GET"])
def get_full_weather_visual():
    full_weather_visual = calculator_functions.full_weather_visual_for_ship(raw_messages_stations,
                                                                            weather_data)
    return jsonify(full_weather_visual.to_json())


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python3_render_template]
# [END gae_python38_render_template]
