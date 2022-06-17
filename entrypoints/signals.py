from pyspark.sql import SparkSession

from de_assignment.main import main

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Signals').getOrCreate()
    main(spark)
    spark.stop()
