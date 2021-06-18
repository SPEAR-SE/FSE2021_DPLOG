from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Test") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    df_csv = spark.read.load("...")
    
    df_csv = df_csv.dropna(subset=['postReleaseBugs']).filter('postReleaseBugs > 0').withColumn('new_postReleaseBugs', F.log(F.col('postReleaseBugs')))
    length = df_csv.count()
    print(length)
    spark.stop()
