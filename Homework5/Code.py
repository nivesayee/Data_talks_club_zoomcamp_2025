# Question1
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("homework").getOrCreate()
print(spark.version) #3.3.2

# Question2
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df = df.repartition(4)
df.write.parquet('yellowout') 

# Question3
from pyspark.sql.functions import *
df.filter(to_date(col("tpep_pickup_datetime"))=="2024-10-15").count() # 128097

# Question4
df.createOrReplaceTempView("yellow")
spark.sql("select max(timestampdiff(HOUR,tpep_pickup_datetime,tpep_dropoff_datetime)) from yellow").show() # 162

# Question6
lookupdf=spark.read.csv("taxi_zone_lookup.csv",header=True)
dfcounts = df.select("PULocationID").groupBy("PULocationID").agg(count("*").alias("count"))
mincount = dfcounts.agg(min("count")).collect()[0][0]
dfcounts.filter(col("count")==mincount).join(lookupdf,[dfcounts.PULocationID==lookupdf.LocationID]).select("Zone").show(truncate=False) # Governor's Island/Ellis Island/Liberty Island
