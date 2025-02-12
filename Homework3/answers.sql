-- creating external table with data from gcs
create or replace external table `atomic-envelope-448820-r9.yellow_data_2024.ext_yd_table_2024`
options (
  format='parquet',
  uris=['gs://yellow_taxi_2024_half/yellow_tripdata_2024-01.parquet','gs://yellow_taxi_2024_half/yellow_tripdata_2024-02.parquet','gs://yellow_taxi_2024_half/yellow_tripdata_2024-03.parquet','gs://yellow_taxi_2024_half/yellow_tripdata_2024-04.parquet','gs://yellow_taxi_2024_half/yellow_tripdata_2024-05.parquet','gs://yellow_taxi_2024_half/yellow_tripdata_2024-06.parquet']
);

-- creating internal bq table
create or replace table atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part
as select * from atomic-envelope-448820-r9.yellow_data_2024.ext_yd_table_2024;

-- question1
select count(*) from atomic-envelope-448820-r9.yellow_data_2024.ext_yd_table_2024;

-- question2
select count(distinct PULocationID) from atomic-envelope-448820-r9.yellow_data_2024.ext_yd_table_2024;

select count(distinct PULocationID) from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part;

--question3
select PULocationID from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part;

select PULocationID, DOLocationID from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part;

--question4
select count(*) from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part
where fare_amount=0;

--question5
create or replace table atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_part_and_clus
partition by date(tpep_dropoff_datetime) 
cluster by VendorID
as 
select * from atomic-envelope-448820-r9.yellow_data_2024.ext_yd_table_2024;

--question6
select distinct vendorid from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part
where tpep_dropoff_datetime>='2024-03-01' and tpep_dropoff_datetime<='2024-03-15';

select distinct vendorid from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_part_and_clus
where tpep_dropoff_datetime>='2024-03-01' and tpep_dropoff_datetime<='2024-03-15';

--question7 
-- external data is in GCP bucket

--question8
-- shouldnt always cluster. gotta cluster only when needed.

--question9
select count(*) from atomic-envelope-448820-r9.yellow_data_2024.yd_table_2024_non_part;
-- it shows 0 B will be processed because it doesnt actually query the table instead it gets it from metadata cos counts are stored in table metadata.


