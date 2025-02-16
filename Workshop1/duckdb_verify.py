import duckdb

# Connect to the DuckDB database
conn = duckdb.connect("ny_taxi_pipeline.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = 'ny_taxi_data'")

# Question 2 - Describe the dataset
conn.sql("DESCRIBE").df()
