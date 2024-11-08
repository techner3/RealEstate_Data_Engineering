import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split,concat_ws,when,udf,regexp_replace,trim,from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType

load_dotenv()

def data_dump():

    try:
       

        spark = SparkSession \
        .builder \
        .appName("MongoDB Streaming Dump") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "10") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

        schema =StructType([
        StructField("Parking", StringType(), True),
        StructField("Direction facing", StringType(), True),
        StructField("Listed by", StringType(), True),
        StructField("Property on", StringType(), True),
        StructField("Listed on", StringType(), True),
        StructField("Brokerage terms", StringType(), True),
        StructField("Bachelors Allowed", StringType(), True),
        StructField("Security Deposit", StringType(), True),
        StructField("Pet Allowed", StringType(), True),
        StructField("Non Vegetarian", StringType(), True),
        StructField("Super Built-Up Area", StringType(), True),
        StructField("Carpet Area", StringType(), True),
        StructField("Bedrooms", StringType(), True),
        StructField("Bathrooms", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Rent", StringType(), True),
        StructField("Amenities", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Flooring type", StringType(), True),
        StructField("Furnishing State", StringType(), True),
        StructField("Available from", StringType(), True),
        StructField("Servant Accomation", StringType(), True),
        StructField("Year of Construction", StringType(), True),
    ])

        kafka_config_str=f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{os.getenv('sasl_plain_username')}' password='{os.getenv('sasl_plain_password')}';"
        df =  spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",os.getenv('bootstrap_servers')) \
        .option("subscribe", os.getenv('topic')) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",kafka_config_str) \
        .load() \
        .selectExpr("CAST(value AS STRING) as value") \
        .select(from_json(col("value"),schema).alias("data")) \
        .select("data.*") \
        
        
        def month_to_num(month):
            month_map = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, 
                    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}  # Define the map
            return month_map.get(month.lower(), None)

        month_to_num_udf = udf(month_to_num)
        columns_to_convert=["Rent","Bedrooms","Bathrooms","Security Deposit","Carpet Area","Super Built-Up Area"]

        df=df.withColumn("Listed On",concat_ws("-",split(col("Listed On"),"-").getItem(0), month_to_num_udf(split(col("Listed On"),"-").getItem(1)),col("Year"))).drop("Year")
        df=df.withColumn("Super Built-Up Area", when(col("Super Built-Up Area").contains("("),trim(regexp_replace(col("Super Built-Up Area"), r"\(.*?\)", ""))).otherwise(trim(col("Super Built-Up Area"))))\
            .withColumn("Carpet Area", when(col("Carpet Area").contains("("),regexp_replace(col("Carpet Area"), r"\(.*?\)", "")).otherwise(col("Carpet Area")))\
            .withColumn("Super Built-Up Area", when(col("Super Built-Up Area").contains("sq.ft"),trim(regexp_replace(col("Super Built-Up Area"), r"sq.ft", ""))).otherwise(trim(col("Super Built-Up Area"))))\
            .withColumn("Carpet Area", when(col("Carpet Area").contains("sq.ft"),trim(regexp_replace(col("Carpet Area"), r"sq.ft", ""))).otherwise(trim(col("Carpet Area"))))\
            .withColumn("Amenities", from_json(col("Amenities"), "array<string>"))\
            .withColumn("Rent",when(col("Rent").contains(","),trim(regexp_replace(col("Rent"), r",", "")))\
                .otherwise(trim(col("Rent"))))

        for column_name in columns_to_convert:
            df = df.withColumn(column_name, trim(col(column_name)).cast("int"))

        query = df.writeStream \
        .outputMode("append") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/mongo_db_checkpoint10") \
        .option('spark.mongodb.connection.uri',os.getenv('mongob_db_connection') ) \
        .option('spark.mongodb.database', 'real_estate') \
        .option('spark.mongodb.collection', 'commonfloor') \
        .option("truncate", False) \
        .start()
        print("Write successfull")

        query.awaitTermination(30)
        
    except KeyboardInterrupt:
        pass

    except Exception as e:
        print(e)

    finally:
        spark.stop()

if __name__ == "__main__":
    data_dump()
