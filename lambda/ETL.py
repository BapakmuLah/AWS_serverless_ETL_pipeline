import os
import time
import requests
import pg8000
import socket
import boto3
from datetime import datetime, timedelta, timezone

API_KEY = os.environ["API_KEY"]

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

# RDS
rds = boto3.client("rds")
DB_INSTANCE_ID = "etl"

DEFAULT_CITIES = ["Jakarta", "Bogor", "Depok", "Tangerang", "Bekasi", "Cirebon", "Serang",
                  "Surabaya", "Bandung", "Medan", "Makassar", "Denpasar", "Malang",
                  "Tasikmalaya", "Semarang", "Yogyakarta", "Solo", "Lampung",
                  "Maluku", "Manado"]

def wait_for_db(host, port, timeout=300):
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                return True
        except:
            if time.time() - start > timeout:
                raise Exception("DB not ready")
            time.sleep(5)

class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    def wait(self):
        now = time.time()
        self.calls = [call for call in self.calls if call > now - self.period]
        if len(self.calls) >= self.max_calls:
            sleep_time = self.calls[0] + self.period - now
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.calls.append(time.time())


def extract_weather(cities):
    limiter = RateLimiter(max_calls=250, period=60)
    extracted = []

    for city in cities:
        limiter.wait()
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"q": f"{city},ID", "units": "metric", "appid": API_KEY}

        response = requests.get(url, params=params, timeout=15)

        if response.status_code != 200:
            print(f"Failed to fetch {city}: Status {response.status_code}")
            continue

        data = response.json()
        extracted.append({
            "city_id": data.get("id"),
            "city": data.get("name"),
            "datetime": data.get("dt"),
            "timezone": data.get("timezone"),
            "longitude": data.get("coord", {}).get("lon"),
            "latitude": data.get("coord", {}).get("lat"),
            "country": data.get("sys", {}).get("country"),
            "weather": (data.get("weather") or [{}])[0].get("main"),
            "weather_description": (data.get("weather") or [{}])[0].get("description"),
            "weather_icon": (data.get("weather") or [{}])[0].get("icon"),
            "temp": data.get("main", {}).get("temp"),
            "feels_like": data.get("main", {}).get("feels_like"),
            "temp_min": data.get("main", {}).get("temp_min"),
            "temp_max": data.get("main", {}).get("temp_max"),
            "pressure": data.get("main", {}).get("pressure"),
            "humidity": data.get("main", {}).get("humidity"),
            "sea_level": data.get("main", {}).get("sea_level"),
            "grnd_level": data.get("main", {}).get("grnd_level"),
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_degree": data.get("wind", {}).get("deg"),
            "sunrise": data.get("sys", {}).get("sunrise"),
            "sunset": data.get("sys", {}).get("sunset"),
            "rain": data.get("rain", {}).get("1h", 0),
            "cloudy": data.get("clouds", {}).get("all"),
            "base": data.get("base"),
            "visibility": data.get("visibility")
        })

    return extracted


def transform_weather(rows):
    transformed = []

    for row in rows:
        # UTC DATETIME
        datetime_utc = datetime.fromtimestamp(row['datetime'], tz=timezone.utc)
        row['datetime_utc'] = datetime_utc.isoformat()

        # Local Time
        tz_offset = timedelta(seconds=row['timezone'])
        datetime_local = datetime_utc + tz_offset
        row['datetime_local'] = datetime_local.isoformat()

        row['date'] = str(datetime_local.date())
        row['year'] = datetime_local.year
        row['month'] = datetime_local.month
        row['day'] = datetime_local.day
        row['hour'] = datetime_local.hour
        row['day_name'] = datetime_local.strftime('%A')
        row['week_of_year'] = datetime_local.isocalendar()[1]

        # Sunrise & Sunset
        sunrise_utc = datetime.fromtimestamp(row['sunrise'], tz=timezone.utc)
        row['sunrise_utc'] = sunrise_utc.isoformat()
        sunrise_local = sunrise_utc + tz_offset
        row['sunrise_local'] = sunrise_local.isoformat()

        sunset_utc = datetime.fromtimestamp(row['sunset'], tz=timezone.utc)
        row['sunset_utc'] = sunset_utc.isoformat()
        sunset_local = sunset_utc + tz_offset
        row['sunset_local'] = sunset_local.isoformat()

        # Daylight
        row['daylight_duration'] = (
            (sunset_local - sunrise_local).total_seconds() / 3600
        )

        # REMOVE ORIGINAL FIELD (SAMA KAYAK LOCAL)
        row.pop('datetime', None)

        transformed.append(row)

    return transformed


def load_to_postgres(rows):
    if not rows:
        print("No data to load.")
        return 0

    conn = pg8000.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    cursor = conn.cursor()

    cols = [
        "city_id","city","timezone","longitude","latitude","country",
        "weather","weather_description","weather_icon","temp","feels_like",
        "temp_min","temp_max","pressure","humidity","sea_level","grnd_level",
        "wind_speed","wind_degree","sunrise","sunset","rain","cloudy","base",
        "visibility","datetime_utc","datetime_local","date","year","month",
        "day","hour","day_name","week_of_year","sunrise_utc","sunrise_local",
        "sunset_utc","sunset_local","daylight_duration"
    ]

    placeholders = ",".join(["%s"] * len(cols))

    insert_sql = f"""
        INSERT INTO airflow_data ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO NOTHING;
    """

    values = [tuple(row.get(c) for c in cols) for row in rows]

    cursor.executemany(insert_sql, values)

    conn.commit()
    cursor.close()
    conn.close()

# TURN OFF RDS AFTER ETL RUN
def stop_rds():
    status = rds.describe_db_instances(
        DBInstanceIdentifier=DB_INSTANCE_ID
    )["DBInstances"][0]["DBInstanceStatus"]

    if status == "available":
        rds.stop_db_instance(DBInstanceIdentifier=DB_INSTANCE_ID)

    print("[DEBUG] RDS Shutdown!")

def lambda_handler(event, context):
    try:
        # TURN ON RDS
        wait_for_db(DB_HOST, DB_PORT)

        cities = DEFAULT_CITIES
        raw_rows = extract_weather(cities)
        transformed_rows = transform_weather(raw_rows)
        load_to_postgres(transformed_rows)

        return "SUCCESS"

    # TURN OFF RDS
    finally:
        stop_rds()

        print()