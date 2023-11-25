import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

#############################################################################

def get_spark(spark_app_name, spark_master):
    spark = SparkSession.builder.appName(spark_app_name)
    if spark_master:
        spark = spark.master(spark_master)
    return spark.getOrCreate()

#############################################################################

def parse(src_path: str, dst_path: str) -> None:
    spark = get_spark(spark_app_name = "dlrm-etl", spark_master=None)
    raw_df = spark.read.format("csv").option("delimiter", "\t").option("compression", "gzip").load(src_path)
    raw_df.write.mode("overwrite").parquet(dst_path)

#############################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-path", type=str)
    parser.add_argument("--dst-path", type=str)
    args = parser.parse_args()
    parse(src_path = args.src_path, dst_path = args.dst_path)

#############################################################################
