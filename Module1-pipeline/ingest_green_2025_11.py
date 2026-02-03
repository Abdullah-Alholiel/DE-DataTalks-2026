import tempfile
import requests
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

DB_URL = 'postgresql://root:root@localhost:5432/ny_taxi'
TARGET_TABLE = 'green_taxi_trips'
URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
BATCH_SIZE = 100_000


def download_to_temp(url: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as f:
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return f.name

def main():
    temp_path = download_to_temp(URL)
    engine = create_engine(DB_URL)
    pf = pq.ParquetFile(temp_path)

    first = True
    with engine.begin() as conn:
        for batch in tqdm(pf.iter_batches(batch_size=BATCH_SIZE), total=(pf.metadata.num_rows // BATCH_SIZE) + 1):
            df = batch.to_pandas()
            if first:
                df.head(0).to_sql(TARGET_TABLE, conn, if_exists='replace', index=False)
                first = False
            df.to_sql(TARGET_TABLE, conn, if_exists='append', index=False)

    print('Ingestion completed into table', TARGET_TABLE)


if __name__ == '__main__':
    main()
