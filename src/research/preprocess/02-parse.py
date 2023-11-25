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

def transform(
    src_path,
    dst_path,
    day = None,
    limit = None,
    dst_files_split = 10,
    dst_write_mode = "overwrite"
):
    spark = get_spark(spark_app_name = "dlrm-etl", spark_master=None)
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    
    limit_subquery = f"limit {limit}" if limit else ""
    where_subquery = f"where day = '{str(day).zfill(2)}'" if day is not None else ""

    raw_df = spark.read.parquet(src_path)
    raw_df.createOrReplaceTempView("raw")
    df = spark.sql(f"""
        select
            cast(concat('2023-01-', cast(day as Int)+1) as Date) as date,
            cast(_c0 as Int) as label,
            cast(_c1 as Int) as f1,
            cast(_c2 as Int) as f2,
            cast(_c3 as Int) as f3,
            cast(_c4 as Int) as f4,
            cast(_c5 as Int) as f5,
            cast(_c6 as Int) as f6,
            cast(_c7 as Int) as f7,
            cast(_c8 as Int) as f8,
            cast(_c9 as Int) as f9,
            cast(_c10 as Int) as f10,
            cast(_c11 as Int) as f11,
            cast(_c12 as Int) as f12,
            cast(_c13 as Int) as f13,
            cast(conv(_c14, 16, 10) as Long) as f14,
            cast(conv(_c15, 16, 10) as Long) as f15,
            cast(conv(_c16, 16, 10) as Long) as f16,
            cast(conv(_c17, 16, 10) as Long) as f17,
            cast(conv(_c18, 16, 10) as Long) as f18,
            cast(conv(_c19, 16, 10) as Long) as f19,
            cast(conv(_c20, 16, 10) as Long) as f20,
            cast(conv(_c21, 16, 10) as Long) as f21,
            cast(conv(_c22, 16, 10) as Long) as f22,
            cast(conv(_c23, 16, 10) as Long) as f23,
            cast(conv(_c24, 16, 10) as Long) as f24,
            cast(conv(_c25, 16, 10) as Long) as f25,
            cast(conv(_c26, 16, 10) as Long) as f26,
            cast(conv(_c27, 16, 10) as Long) as f27,
            cast(conv(_c28, 16, 10) as Long) as f28,
            cast(conv(_c29, 16, 10) as Long) as f29,
            cast(conv(_c30, 16, 10) as Long) as f30,
            cast(conv(_c31, 16, 10) as Long) as f31,
            cast(conv(_c32, 16, 10) as Long) as f32,
            cast(conv(_c33, 16, 10) as Long) as f33,
            cast(conv(_c34, 16, 10) as Long) as f34,
            cast(conv(_c35, 16, 10) as Long) as f35,
            cast(conv(_c36, 16, 10) as Long) as f36,
            cast(conv(_c37, 16, 10) as Long) as f37,
            cast(conv(_c38, 16, 10) as Long) as f38,
            cast(conv(_c39, 16, 10) as Long) as f39
        from raw
        {where_subquery}
        {limit_subquery}
    """)
    df.repartition(dst_files_split).write.mode(dst_write_mode).partitionBy("date").parquet(dst_path)


#############################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-path", type=str, required=True)
    parser.add_argument("--dst-path", type=str, required=True)
    parser.add_argument("--day", type=str, default=None)

    args = parser.parse_args()
    transform(src_path = args.src_path, dst_path = args.dst_path, day = args.day)

#############################################################################
