from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, max as _max
from utils.etl_helpers import calculating_clicks, calculating_conversion, calculating_qualified, calculating_unqualified, upsert_partition
import mysql.connector
import pyspark

def build_spark_session():
    return SparkSession.builder \
        .appName("BatchETL_Cassandra_to_MySQL") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

if __name__ == "__main__":
    spark = build_spark_session()
    mysql_latest_str = "1998-01-01 23:59:59"

    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="tracking_new", keyspace="my_keyspace")
        .load()
        .where(col("ts") >= mysql_latest_str)
    )

    if df.rdd.isEmpty():
        print("No new data")
        spark.stop()
        exit(0)

    df = df.withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
    df = df.select("create_time", "ts", "job_id", "custom_track", "bid", "campaign_id", "group_id", "publisher_id").filter(col("job_id").isNotNull())

    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)

    cassandra_output = clicks_output.join(conversion_output, ["job_id","date","hour","publisher_id","campaign_id","group_id"], "full_outer") \
        .join(qualified_output, ["job_id","date","hour","publisher_id","campaign_id","group_id"], "full_outer") \
        .join(unqualified_output, ["job_id","date","hour","publisher_id","campaign_id","group_id"], "full_outer")

    # Enrich
    jdbc_url = "jdbc:mysql://mysql:3306/my_sql"
    company_df = spark.read.format("jdbc").options(url=jdbc_url, driver="com.mysql.cj.jdbc.Driver", dbtable="(SELECT job_id, company_id, group_id, campaign_id FROM job) AS job_tbl", user="airflow", password="airflow").load()

    final_output = cassandra_output.join(company_df, "job_id", "left").drop(company_df.group_id).drop(company_df.campaign_id)
    final_output = final_output.withColumn("sources", lit("Cassandra")).withColumnRenamed("date", "dates").withColumnRenamed("hour", "hours").withColumnRenamed("unqualified", "disqualified_application").withColumnRenamed("qualified", "qualified_application").withColumnRenamed("conversions", "conversion")

    conn_params = {"host":"mysql", "port":3306, "user":"airflow", "password":"airflow", "database":"my_sql"}
    final_output.foreachPartition(lambda rows: upsert_partition(rows, conn_params))
    spark.stop()
