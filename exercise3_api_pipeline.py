import dlt 
import requests
from pathlib import Path 
import os 
import datetime 
from dotenv import load_dotenv

load_dotenv()
weather_api_key = os.getenv("WEATHER_API_KEY")

@dlt.resource(write_disposition="replace")
def fetch_weather(cities):
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={weather_api_key}&units=metric"
        response = requests.get(url)
        print(response)

        if response.status_code == 200: 
            data = response.json()
            print(data)
            timestamp = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
            yield {"city": data.get("name"), "timestamp": timestamp, "temperature": data("min").get("temp"), "humidity": data("min").get("humidity"), "pressure": data("main").get("pressure") }


def run_pipeline(cities, table_name): 

    db_path = working_directory / "weather.duckdb"
    pipeline = dlt.pipeline(pipeline_name = "weather", destination = dlt.destinations.duckdb(str(db_path)), dataset_name = "staging")

    load_info = pipeline.run(fetch_weather(cities), table_name = table_name)
    print(load_info)

if __name__ == "__main__": 
    working_directory = Path(__file__).parent
    os.chdir(working_directory)
    cities = ["London"]
    table_name = "currrent_weather"
    run_pipeline(cities, table_name)
    print(weather_api_key)