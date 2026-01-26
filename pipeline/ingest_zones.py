import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# URL for the Taxi Zone Lookup CSV
zones_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

df_zones = pd.read_csv(zones_url)

# Ingest it into a table named 'zones'
df_zones.to_sql(name='zones', con=engine, if_exists='replace')

print("Zones table ingested successfully!")