import datetime
import argparse
from itertools import chain

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def pyspark_transform(input_path, output_path, year, month):
    first_of_month = datetime.date(year, month, 1).strftime("%Y-%m-%d")

    spark = SparkSession.builder.getOrCreate()

    # Read data
    fact_table = spark.read.parquet(input_path)

    # Create datetime_dim table
    datetime_dim = (
        spark.sql(
            f"""
                 select
                     sequence(to_timestamp('{first_of_month}'),
                              to_timestamp('{first_of_month}') + interval 1 month - interval 1 hour,
                              interval 1 hour) as datetime
                 """
        )
        .withColumn("datetime", F.explode("datetime"))
        .select(
            F.date_format("datetime", "yyMMdd-HH").alias("datetime_id"),
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
            F.hour("datetime").alias("hour"),
            F.dayofweek("datetime").alias("weekday"),
        )
        .orderBy("year", "month", "day", "hour")
    )

    # Create hvfhs_license_dim table
    hvfhs_license_dim = (
        fact_table.select(F.col("hvfhs_license_num"))
        .distinct()
        .orderBy("hvfhs_license_num")
        .select(
            F.md5("hvfhs_license_num").alias("hvfhs_license_id"), "hvfhs_license_num"
        )
    )

    # Create a base_dim table
    org_base_num = fact_table.select(F.col("originating_base_num").alias("base_num"))
    dp_base_num = fact_table.select(F.col("dispatching_base_num").alias("base_num"))
    base_dim = (
        org_base_num.union(dp_base_num)
        .distinct()
        .dropna()
        .orderBy("base_num")
        .select(F.md5("base_num").alias("base_num_id"), "base_num")
    )

    # Transform fact_table
    hvfhs_license_num = hvfhs_license_dim.rdd.map(
        lambda x: x.hvfhs_license_num
    ).collect()
    hvfhs_license_id = hvfhs_license_dim.rdd.map(lambda x: x.hvfhs_license_id).collect()
    hvfhs_license_map = F.create_map(
        [F.lit(c) for c in chain(*zip(hvfhs_license_num, hvfhs_license_id))]
    )

    base_num = base_dim.rdd.map(lambda x: x.base_num).collect()
    base_num_id = base_dim.rdd.map(lambda x: x.base_num_id).collect()
    base_map = F.create_map([F.lit(c) for c in chain(*zip(base_num, base_num_id))])

    norm_date_fact_table = (
        fact_table.drop("request_datetime", "on_scene_datetime")
        .withColumns(
            {
                "pickup_datetime": F.date_format("pickup_datetime", "yyMMdd-HH"),
                "dropoff_datetime": F.date_format("dropoff_datetime", "yyMMdd-HH"),
            }
        )
        .withColumnRenamed("pickup_datetime", "pickup_datetime_id")
        .withColumnRenamed("dropoff_datetime", "dropoff_datetime_id")
    )

    norm_base_fact_table = (
        norm_date_fact_table.withColumns(
            {
                "originating_base_num": base_map[F.col("originating_base_num")],
                "dispatching_base_num": base_map[F.col("dispatching_base_num")],
            }
        )
        .withColumnRenamed("originating_base_num", "originating_base_id")
        .withColumnRenamed("dispatching_base_num", "dispatching_base_id")
    )

    norm_hvfhs_fact_table = norm_base_fact_table.withColumn(
        "hvfhs_license_num", hvfhs_license_map[F.col("hvfhs_license_num")]
    ).withColumnRenamed("hvfhs_license_num", "hvfhs_license_id")

    datetime_dim_path = output_path + "datetime_dim"
    base_dim_path = output_path + "base_dim"
    hvfhs_license_dim_path = output_path + "hvfhs_license_dim"
    fact_table_path = output_path + "fact_table"

    def write_non_date_dim(new_dim_table, path):
        """
        If there was a previous version of non-datetime dim, we deduplicate
        records of both versions then overwrite the old version
        """
        try:
            old_dim_table = spark.read.parquet(path)
            new_dim_table = old_dim_table.union(new_dim_table).distinct()
        except:
            pass
        finally:
            new_dim_table.write.parquet(path, mode="overwrite")

    datetime_dim.write.parquet(datetime_dim_path, mode="overwrite")
    write_non_date_dim(base_dim, base_dim_path)
    write_non_date_dim(hvfhs_license_dim, hvfhs_license_dim_path)
    norm_hvfhs_fact_table.write.parquet(fact_table_path, mode="overwrite")


arg_parser = argparse.ArgumentParser(prog="PySpark Transform Job")
arg_parser.add_argument("--nyc_input_path", type=str, required=True)
arg_parser.add_argument("--nyc_output_path", type=str, required=True)
arg_parser.add_argument("--nyc_year", type=int, required=True)
arg_parser.add_argument("--nyc_month", type=int, required=True)

args = arg_parser.parse_args()
pyspark_transform(
    args.nyc_input_path, args.nyc_output_path, args.nyc_year, args.nyc_month
)
