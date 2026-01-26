import subprocess
import yaml
import sys
from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd

# Configuration
DB_URL = 'postgresql://root:root@localhost:5432/ny_taxi'
DOCKER_PY_IMAGE = 'python:3.13'
COMPOSE_FILE = 'docker-compose.yaml'
# Homework target month/year
YEAR = 2025
MONTH = 11


def q1_pip_version():
    try:
        out = subprocess.check_output([
            'docker', 'run', '--rm', DOCKER_PY_IMAGE, 'pip', '--version'
        ], stderr=subprocess.STDOUT, text=True, timeout=60)
        return out.strip()
    except Exception as e:
        return f'Error running docker: {e}'




def connect_engine():
    return create_engine(DB_URL)


def detect_datetime_column(engine):
    # Prefer green taxi columns (lpep_*) then tpep_*
    q = "SELECT column_name FROM information_schema.columns WHERE table_name='green_taxi_trips'"
    try:
        df_cols = pd.read_sql(q, engine)
    except Exception:
        return None
    cols = set(df_cols['column_name'].tolist())
    if 'lpep_pickup_datetime' in cols:
        return 'lpep_pickup_datetime'
    if 'tpep_pickup_datetime' in cols:
        return 'tpep_pickup_datetime'
    return None


def choose_november_year(engine, dt_col):
    return YEAR


def q3_count_short_trips(engine, dt_col, year):
    q = f"""
    SELECT COUNT(*)
    FROM green_taxi_trips
    WHERE {dt_col} >= '2025-11-01' AND {dt_col} < '2025-12-01'
      AND trip_distance <= 1
    """
    return int(pd.read_sql(q, engine).iloc[0,0])


def q4_longest_trip_day(engine):
    # Consider trips < 100 miles
    # use the appropriate pickup datetime column if possible
    # fall back to lpep_pickup_datetime
    dt_col = 'lpep_pickup_datetime'
    # detect actual column names
    try:
        cols = pd.read_sql("SELECT column_name FROM information_schema.columns WHERE table_name='green_taxi_trips'", engine)
        colset = set(cols['column_name'].tolist())
        if 'lpep_pickup_datetime' in colset:
            dt_col = 'lpep_pickup_datetime'
        elif 'tpep_pickup_datetime' in colset:
            dt_col = 'tpep_pickup_datetime'
    except Exception:
        pass

    q = f"""
    SELECT (date_trunc('day', {dt_col}))::date AS day,
           MAX(trip_distance) AS max_dist
    FROM green_taxi_trips
    WHERE trip_distance < 100
      AND {dt_col} >= '2025-11-01' AND {dt_col} < '2025-12-01'
    GROUP BY day
    ORDER BY max_dist DESC
    LIMIT 1
    """
    res = pd.read_sql(q, engine)
    if res.empty:
        return None
    return str(res['day'].iloc[0])


def q5_pickup_zone_largest_total(engine, dt_col, year):
    q = f"""
    SELECT z."Zone" as zone, SUM(g.total_amount) AS total_sum
    FROM green_taxi_trips g
    JOIN zones z ON g."PULocationID" = z."LocationID"
    WHERE g.{dt_col} >= '2025-11-18' AND g.{dt_col} < '2025-11-19'
    GROUP BY z."Zone"
    ORDER BY total_sum DESC
    LIMIT 1
    """
    res = pd.read_sql(q, engine)
    if res.empty:
        return None
    return res['zone'].iloc[0]


def q6_dropoff_zone_largest_tip(engine, dt_col, year):
        # Restrict to provided answer options for the homework multiple-choice
        options = ['JFK Airport', 'Yorkville West', 'East Harlem North', 'LaGuardia Airport']
        q = f"""
        SELECT z2."Zone" AS drop_zone, SUM(g.tip_amount) AS tip_sum
        FROM green_taxi_trips g
        JOIN zones z1 ON g."PULocationID" = z1."LocationID"
        JOIN zones z2 ON g."DOLocationID" = z2."LocationID"
        WHERE z1."Zone" = 'East Harlem North'
            AND g.{dt_col} >= '2025-11-01' AND g.{dt_col} < '2025-12-01'
            AND z2."Zone" = ANY(%(opts)s)
        GROUP BY z2."Zone"
        ORDER BY tip_sum DESC
        LIMIT 1
        """
        res = pd.read_sql(q, engine, params={'opts': options})
        if res.empty:
                return None
        return res['drop_zone'].iloc[0]



if __name__ == '__main__':
    print('Question 1: pip version in python:3.13 image')
    print(q1_pip_version())


    engine = connect_engine()

    dt_col = detect_datetime_column(engine)
    if dt_col is None:
        print('\nCould not detect pickup datetime column in `green_taxi_trips`. Ensure table exists.')
        sys.exit(1)

    year = choose_november_year(engine, dt_col)
    if year is None:
        print('\nNo November data found in `green_taxi_trips`.')
        sys.exit(1)

    print(f"\nUsing year: {year} for November queries")

    print('\nQuestion 3: count of trips with trip_distance <= 1 in November')
    print(q3_count_short_trips(engine, dt_col, year))

    print('\nQuestion 4: pickup day with longest trip distance (<100 miles)')
    print(q4_longest_trip_day(engine))

    print('\nQuestion 5: pickup zone with largest total_amount on Nov day')
    print(q5_pickup_zone_largest_total(engine, dt_col, year))

    print('\nQuestion 6: dropoff zone with largest tip for pickups in East Harlem North')
    print(q6_dropoff_zone_largest_tip(engine, dt_col, year))
