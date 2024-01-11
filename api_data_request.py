import requests
import pandas as pd
from datetime import datetime, timedelta
import time

class WeatherStations:
    def __init__(self):
        self.stations = None
        self.station_name = None

    def stations_request(self):
        response = requests.get('https://api.meteo.lt/v1/stations')
        if response.status_code == 200:
            self.stations_parsing(response)
        else:
            print(f"Error: {response.status_code} - {response.text}")

    def stations_parsing(self, response):
        json_data = response.json()
        self.stations = [i['code'] for i in json_data]
        self.station_name = [i['name'] for i in json_data]

    def stations_information(self):
        return f"{self.stations} \n {self.station_name}"

class WeatherData(WeatherStations):
    def __init__(self):
        super().__init__()
        self.df = None
        self.rows = []

    def request(self):
        self.stations_request()
        start_date = datetime.strptime("2021-01-01", "%Y-%m-%d")
        end_date = datetime.strptime("2021-01-03", "%Y-%m-%d")
        request_counter = 0
        while end_date >= start_date:
            print(start_date)
            for station, station_name in zip(self.stations, self.station_name):
                url = f'https://api.meteo.lt/v1/stations/{station}/observations/{start_date.strftime("%Y-%m-%d")}'
                retry_count = 0
                while retry_count < 12:
                    try:
                        response = requests.get(url)
                        response.raise_for_status()
                        break
                    except requests.exceptions.RequestException as e:
                        print(f"Error for station {station_name}: {e}")
                        retry_count += 1
                        time.sleep(5)
                if retry_count == 12:
                    print(f"Max retries reached for station {station_name}. Skipping.")
                    continue
                request_counter += 1
                self.weather_parsing(station_name, response)
                if request_counter == 162:
                    request_counter = 0
                    time.sleep(60)
            start_date += timedelta(days=1)
        self.data_saving()

    def weather_parsing(self, station_name, response):
        json_data = response.json()
        for i in json_data['observations']:
            row = [
                station_name,
                i.get('observationTimeUtc', 'N/A'),
                i.get('airTemperature', 'N/A'),
                i.get('feelsLikeTemperature', 'N/A'),
                i.get('windSpeed', 'N/A'),
                i.get('windGust', 'N/A'),
                i.get('windDirection', 'N/A'),
                i.get('cloudCover', 'N/A'),
                i.get('seaLevelPressure', 'N/A'),
                i.get('relativeHumidity', 'N/A'),
                i.get('precipitation', 'N/A'),
                i.get('conditionCode', 'N/A')]
            self.rows.append(row)

    def data_saving(self):
        columns = [
            'Station Name',
            'Observation Time (UTC)',
            'Air Temperature',
            'Feels Like Temperature',
            'Wind Speed',
            'Wind Gust',
            'Wind Direction',
            'Cloud Cover',
            'Sea Level Pressure',
            'Relative Humidity',
            'Precipitation',
            'Condition Code']
        self.df = pd.DataFrame(self.rows, columns=columns)
        self.df.to_csv('weather_data.csv', index=False)

    def df_information(self):
        return f"{self.df}"

weather_data_instance = WeatherData()
weather_data_instance.request()
print(weather_data_instance.df_information())