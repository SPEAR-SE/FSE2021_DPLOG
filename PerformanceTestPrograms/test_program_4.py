from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Test") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    df_csv = spark.read.load("...")

    df_csv = df_csv.drop('classname').fillna(0, subset=['postReleaseBugs']).filter('postReleaseBugs = 0')
    length = df_csv.count()
    print(length)
    spark.stop()
