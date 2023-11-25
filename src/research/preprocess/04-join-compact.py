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

def join(
        src_path_logs, 
        src_path_dicts, 
        dst_path,
        date = None,
        repartition = None,
        freq_threshold_abs = None,
        freq_threshold_pct = None,
    ):
    spark = get_spark(spark_app_name = "dlrm-etl", spark_master=None)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    
    src_data_logs = spark.read.parquet(src_path_logs)
    src_data_dicts = spark.read.parquet(src_path_dicts)
    
    src_data_logs.createOrReplaceTempView("src_data_logs")
    src_data_dicts.createOrReplaceTempView("src_data_dicts")

    freq_cond = []
    if freq_threshold_abs is not None:
        freq_cond.append(f"frequency_rank <= {freq_threshold_abs}")
    if freq_threshold_pct is not None:
        freq_cond.append(f"frequency_cumsum_share_pct <= {freq_threshold_pct}")
    freq_cond = " AND ".join(freq_cond) if freq_cond else "1=1"
    
    where_cond = f"WHERE l.date = '{date}'" if date is not None else ""

    dst_data = spark.sql(f"""
        select
            l.date as date,
            l.label as label,
            l.f1 as f1,
            l.f2 as f2,
            l.f3 as f3,
            l.f4 as f4,
            l.f5 as f5,
            l.f6 as f6,
            l.f7 as f7,
            l.f8 as f8,
            l.f9 as f9,
            l.f10 as f10,
            l.f11 as f11,
            l.f12 as f12,
            l.f13 as f13,
            if(f14 is null, 0, if(d14.idx is null, 1, d14.idx + 1)) as f14_idx,
            if(f15 is null, 0, if(d15.idx is null, 1, d15.idx + 1)) as f15_idx,
            if(f16 is null, 0, if(d16.idx is null, 1, d16.idx + 1)) as f16_idx,
            if(f17 is null, 0, if(d17.idx is null, 1, d17.idx + 1)) as f17_idx,
            if(f18 is null, 0, if(d18.idx is null, 1, d18.idx + 1)) as f18_idx,
            if(f19 is null, 0, if(d19.idx is null, 1, d19.idx + 1)) as f19_idx,
            if(f20 is null, 0, if(d20.idx is null, 1, d20.idx + 1)) as f20_idx,
            if(f21 is null, 0, if(d21.idx is null, 1, d21.idx + 1)) as f21_idx,
            if(f22 is null, 0, if(d22.idx is null, 1, d22.idx + 1)) as f22_idx,
            if(f23 is null, 0, if(d23.idx is null, 1, d23.idx + 1)) as f23_idx,
            if(f24 is null, 0, if(d24.idx is null, 1, d24.idx + 1)) as f24_idx,
            if(f25 is null, 0, if(d25.idx is null, 1, d25.idx + 1)) as f25_idx,
            if(f26 is null, 0, if(d26.idx is null, 1, d26.idx + 1)) as f26_idx,
            if(f27 is null, 0, if(d27.idx is null, 1, d27.idx + 1)) as f27_idx,
            if(f28 is null, 0, if(d28.idx is null, 1, d28.idx + 1)) as f28_idx,
            if(f29 is null, 0, if(d29.idx is null, 1, d29.idx + 1)) as f29_idx,
            if(f30 is null, 0, if(d30.idx is null, 1, d30.idx + 1)) as f30_idx,
            if(f31 is null, 0, if(d31.idx is null, 1, d31.idx + 1)) as f31_idx,
            if(f32 is null, 0, if(d32.idx is null, 1, d32.idx + 1)) as f32_idx,
            if(f33 is null, 0, if(d33.idx is null, 1, d33.idx + 1)) as f33_idx,
            if(f34 is null, 0, if(d34.idx is null, 1, d34.idx + 1)) as f34_idx,
            if(f35 is null, 0, if(d35.idx is null, 1, d35.idx + 1)) as f35_idx,
            if(f36 is null, 0, if(d36.idx is null, 1, d36.idx + 1)) as f36_idx,
            if(f37 is null, 0, if(d37.idx is null, 1, d37.idx + 1)) as f37_idx,
            if(f38 is null, 0, if(d38.idx is null, 1, d38.idx + 1)) as f38_idx,
            if(f39 is null, 0, if(d39.idx is null, 1, d39.idx + 1)) as f39_idx
        from src_data_logs as l
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f14' and ({freq_cond})) as d14 on (l.f14 = d14.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f15' and ({freq_cond})) as d15 on (l.f15 = d15.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f16' and ({freq_cond})) as d16 on (l.f16 = d16.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f17' and ({freq_cond})) as d17 on (l.f17 = d17.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f18' and ({freq_cond})) as d18 on (l.f18 = d18.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f19' and ({freq_cond})) as d19 on (l.f19 = d19.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f20' and ({freq_cond})) as d20 on (l.f20 = d20.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f21' and ({freq_cond})) as d21 on (l.f21 = d21.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f22' and ({freq_cond})) as d22 on (l.f22 = d22.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f23' and ({freq_cond})) as d23 on (l.f23 = d23.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f24' and ({freq_cond})) as d24 on (l.f24 = d24.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f25' and ({freq_cond})) as d25 on (l.f25 = d25.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f26' and ({freq_cond})) as d26 on (l.f26 = d26.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f27' and ({freq_cond})) as d27 on (l.f27 = d27.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f28' and ({freq_cond})) as d28 on (l.f28 = d28.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f29' and ({freq_cond})) as d29 on (l.f29 = d29.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f30' and ({freq_cond})) as d30 on (l.f30 = d30.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f31' and ({freq_cond})) as d31 on (l.f31 = d31.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f32' and ({freq_cond})) as d32 on (l.f32 = d32.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f33' and ({freq_cond})) as d33 on (l.f33 = d33.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f34' and ({freq_cond})) as d34 on (l.f34 = d34.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f35' and ({freq_cond})) as d35 on (l.f35 = d35.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f36' and ({freq_cond})) as d36 on (l.f36 = d36.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f37' and ({freq_cond})) as d37 on (l.f37 = d37.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f38' and ({freq_cond})) as d38 on (l.f38 = d38.key)
        left join (select key, frequency_rank as idx, frequency_cumsum_share_pct as pct from src_data_dicts where feature = 'f39' and ({freq_cond})) as d39 on (l.f39 = d39.key)
        {where_cond}
    """)
    if repartition is not None:
        dst_data = dst_data.repartition(repartition)
    dst_data.write.partitionBy("date").mode("overwrite").parquet(dst_path)


#############################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-path-logs", type=str, required=True)
    parser.add_argument("--src-path-dicts", type=str, required=True)
    parser.add_argument("--dst-path", type=str, required=True),
    parser.add_argument("--date", type=str, required=False),
    parser.add_argument("--repartition", type=int, required=False)
    parser.add_argument("--freq-threshold-abs", type=int, required=False),
    parser.add_argument("--freq-threshold-pct", type=float, required=False),
    args = parser.parse_args()
    join(
        src_path_logs = args.src_path_logs,
        src_path_dicts = args.src_path_dicts,
        dst_path = args.dst_path,
        date = args.date,
        repartition = args.repartition,
        freq_threshold_abs = args.freq_threshold_abs,
        freq_threshold_pct = args.freq_threshold_pct
    )

#############################################################################
