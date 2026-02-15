import duckdb
import requests
from pathlib import Path
import sys

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# This script should be run from within the taxi_rides_ny/ directory
# so that the DuckDB file is created there.

def download_file(url, filepath):
    """Download a file with streaming to handle large files."""
    print(f"  Downloading {url} ...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def download_and_convert(taxi_type, years):
    """Download CSV.gz files, convert to Parquet."""
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            try:
                download_file(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", csv_gz_filepath)
            except Exception as e:
                print(f"  ERROR downloading {csv_gz_filename}: {e}")
                continue

            print(f"  Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            # Remove CSV.gz to save space
            csv_gz_filepath.unlink()
            print(f"  Completed {parquet_filename}")


def load_into_duckdb(db_path):
    """Load all Parquet files into the DuckDB database."""
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    # Load green and yellow taxi data
    for taxi_type in ["yellow", "green"]:
        data_dir = Path("data") / taxi_type
        if not data_dir.exists():
            print(f"WARNING: {data_dir} does not exist, skipping {taxi_type}")
            continue
        print(f"Loading {taxi_type} data into DuckDB...")
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
        """)
        count = con.execute(f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata").fetchone()[0]
        print(f"  {taxi_type}_tripdata: {count:,} rows loaded")

    # Load FHV data
    fhv_dir = Path("data") / "fhv"
    if fhv_dir.exists():
        print("Loading fhv data into DuckDB...")
        con.execute("""
            CREATE OR REPLACE TABLE prod.fhv_tripdata AS
            SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
        """)
        count = con.execute("SELECT COUNT(*) FROM prod.fhv_tripdata").fetchone()[0]
        print(f"  fhv_tripdata: {count:,} rows loaded")

    con.close()
    print("All data loaded successfully!")


def update_gitignore():
    """Add data/ and .duckdb files to .gitignore."""
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""
    additions = []
    if "data/" not in content:
        additions.append("data/")
    if "*.duckdb" not in content:
        additions.append("*.duckdb")
        additions.append("*.duckdb.wal")
    if additions:
        with open(gitignore_path, "a") as f:
            f.write("\n# Local data and DuckDB files\n")
            for a in additions:
                f.write(a + "\n")


if __name__ == "__main__":
    update_gitignore()

    print("=" * 60)
    print("STEP 1: Download & convert Green + Yellow taxi data (2019-2020)")
    print("=" * 60)
    for taxi_type in ["yellow", "green"]:
        print(f"\n--- {taxi_type.upper()} ---")
        download_and_convert(taxi_type, years=[2019, 2020])

    print("\n" + "=" * 60)
    print("STEP 2: Download & convert FHV data (2019)")
    print("=" * 60)
    download_and_convert("fhv", years=[2019])

    print("\n" + "=" * 60)
    print("STEP 3: Load all data into DuckDB")
    print("=" * 60)
    load_into_duckdb("taxi_rides_ny.duckdb")

    print("\nDone! You can now run dbt.")