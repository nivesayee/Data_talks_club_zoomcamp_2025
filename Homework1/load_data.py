import pandas as pd
from sqlalchemy import create_engine


db_connection_string = 'postgresql://postgres:postgres@db:5432/ny_taxi'

engine = create_engine(db_connection_string)


data_file = '/app/green_tripdata_2019-10.csv'

df = pd.read_csv(data_file)
df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)

df.to_sql(name="greentripdata",con=engine,if_exists="replace")

lookup_file = '/app/taxi_zone_lookup.csv'

lookup_df = pd.read_csv(lookup_file)
lookup_df.to_sql(name="lookup",con=engine,if_exists="replace")