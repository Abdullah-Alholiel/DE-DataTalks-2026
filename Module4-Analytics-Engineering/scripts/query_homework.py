import duckdb

DB_PATH = "taxi_rides_ny.duckdb"

def run():
    con = duckdb.connect(DB_PATH, read_only=True)

    print("=" * 60)
    print("HOMEWORK ANSWERS")
    print("=" * 60)

    # --- Q1 (Conceptual) ---
    print("\n--- Q1: dbt Lineage and Execution ---")
    print("Answer: int_trips_unioned only")
    print("  (dbt run --select <model> builds ONLY that model,")
    print("   not its upstream or downstream dependencies.)")

    # --- Q2 (Conceptual) ---
    print("\n--- Q2: dbt Tests ---")
    print("Answer: dbt will fail the test, returning a non-zero exit code")
    print("  (Value 6 is not in accepted_values [1,2,3,4,5].)")

    # --- Q3: Count of records in fct_monthly_zone_revenue ---
    print("\n--- Q3: Count of records in fct_monthly_zone_revenue ---")
    result = con.execute("SELECT COUNT(*) FROM prod.fct_monthly_zone_revenue").fetchone()[0]
    print(f"Answer: {result:,}")

    # --- Q4: Best performing zone for Green taxis (2020) ---
    print("\n--- Q4: Best performing zone for Green taxis (2020) ---")
    result = con.execute("""
        SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
        FROM prod.fct_monthly_zone_revenue
        WHERE service_type = 'Green'
          AND revenue_month >= '2020-01-01'
          AND revenue_month < '2021-01-01'
        GROUP BY pickup_zone
        ORDER BY total_revenue DESC
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"  {row[0]}: ${row[1]:,.2f}")
    print(f"Answer: {result[0][0]}")

    # --- Q5: Green taxi trip counts (October 2019) ---
    print("\n--- Q5: Green taxi trip counts (October 2019) ---")
    result = con.execute("""
        SELECT SUM(total_monthly_trips) as total_trips
        FROM prod.fct_monthly_zone_revenue
        WHERE service_type = 'Green'
          AND revenue_month = '2019-10-01'
    """).fetchone()[0]
    print(f"Answer: {result:,}")

    # --- Q6: Count of records in stg_fhv_tripdata ---
    print("\n--- Q6: Count of records in stg_fhv_tripdata ---")
    try:
        result = con.execute("SELECT COUNT(*) FROM prod.stg_fhv_tripdata").fetchone()[0]
        print(f"Answer: {result:,}")
    except Exception as e:
        # If the model was built in dev schema
        try:
            result = con.execute("SELECT COUNT(*) FROM dev.stg_fhv_tripdata").fetchone()[0]
            print(f"Answer: {result:,}")
        except:
            print(f"ERROR: stg_fhv_tripdata not found. Did you run dbt for the FHV model?")
            print(f"  Original error: {e}")

    con.close()

if __name__ == "__main__":
    run()