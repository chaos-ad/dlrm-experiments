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

def build_dictionary(
    feature_name,
    src_path,
    dst_path,
    dst_write_mode = "overwrite"
):
    spark = get_spark(spark_app_name = "dlrm-etl", spark_master=None)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    src_data = spark.read.parquet(src_path)
    src_data.createOrReplaceTempView("src_data")
    dst_data = spark.sql(f"""
        SELECT
            '{feature_name}' as feature,
            key,
            frequency,
            frequency_rank,
            frequency / frequency_total as frequency_share_pct,
            frequency_cumsum / frequency_total as frequency_cumsum_share_pct
        FROM (
            select
                key,
                frequency,
                row_number() OVER (ORDER BY frequency DESC) AS frequency_rank,
                sum(frequency) OVER () AS frequency_total,
                sum(frequency) OVER (ORDER BY frequency DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS frequency_cumsum
            from (
                select
                    coalesce({feature_name}, 0) as key,
                    count(*) as frequency
                from src_data
                where {feature_name} is not null
                group by {feature_name}
            ) as a
        ) as b
        ORDER BY frequency DESC
    """)
    dst_data.write.partitionBy("feature").mode(dst_write_mode).parquet(dst_path)


#############################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-name", type=str, required=True)
    parser.add_argument("--src-path", type=str, required=True)
    parser.add_argument("--dst-path", type=str, required=True)
    args = parser.parse_args()
    build_dictionary(
        feature_name = args.feature_name,
        src_path = args.src_path, 
        dst_path = args.dst_path
    )

#############################################################################
