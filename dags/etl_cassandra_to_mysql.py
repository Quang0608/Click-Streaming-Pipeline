import datetime as dt
from typing import Optional
import sys, os
sys.path.append("/opt/airflow/dags")

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mysql.hooks.mysql import MySqlHook


# -----------------------------
CASSANDRA_KEYSPACE = Variable.get("cassandra_keyspace", default_var="my_keyspace")
CASSANDRA_TABLE = Variable.get("cassandra_table", default_var="tracking_new")

MYSQL_CONN_ID = Variable.get("mysql_conn_id", default_var="mysql_default")
MYSQL_DB = Variable.get("mysql_db", default_var="my_sql")
MYSQL_EVENTS_TABLE = Variable.get("mysql_events_table", default_var="events")

# JAR paths / packages (kept as variables to allow override)
MYSQL_JDBC_JAR = Variable.get(
    "mysql_jdbc_jar",
    default_var="/home/quangnm/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar",
)
CASSANDRA_SPARK_CONNECTOR = Variable.get(
    "cassandra_spark_connector",
    default_var="com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
)

# --------------Build spark session---------------
def build_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("Airflow-Spark-Cassandra-to-MySQL")
        .config("spark.jars.packages", CASSANDRA_SPARK_CONNECTOR)
        .config("spark.jars", "/opt/airflow/jars/mysql-connector-j-8.0.33.jar")
        .config("spark.cassandra.connection.host", "cassandra-2")
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )
    return spark


# -------------Calculating metrics----------------
def calculating_clicks(df):
    from pyspark.sql.functions import col, count, avg, sum as _sum, date_format, hour

    df_clicks = (
        df.filter(col("custom_track") == "click")
          .na.fill({"bid": 0, "job_id": "0", "publisher_id": "0", "group_id": "0", "campaign_id": "0"})
          .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
          .withColumn("hour", hour(col("ts")))
    )

    agg = (
        df_clicks.groupBy("job_id", "date", "hour", "publisher_id", "campaign_id", "group_id")
                 .agg(
                     avg(col("bid")).alias("bid_set"),
                     count("*").alias("clicks"),
                     _sum(col("bid")).alias("spend_hour"),
                 )
    )
    return agg

def calculating_conversion(df):
    from pyspark.sql.functions import col, count, date_format, hour

    df_conv = (
        df.filter(col("custom_track") == "conversion")
          .na.fill({"job_id": "0", "publisher_id": "0", "group_id": "0", "campaign_id": "0"})
          .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
          .withColumn("hour", hour(col("ts")))
    )

    agg = (
        df_conv.groupBy("job_id", "date", "hour", "publisher_id", "campaign_id", "group_id")
               .agg(count("*").alias("conversions"))
    )
    return agg

def calculating_qualified(df):
    from pyspark.sql.functions import col, count, date_format, hour

    df_q = (
        df.filter(col("custom_track") == "qualified")
          .na.fill({"job_id": "0", "publisher_id": "0", "group_id": "0", "campaign_id": "0"})
          .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
          .withColumn("hour", hour(col("ts")))
    )

    agg = (
        df_q.groupBy("job_id", "date", "hour", "publisher_id", "campaign_id", "group_id")
            .agg(count("*").alias("qualified"))
    )
    return agg

def calculating_unqualified(df):
    from pyspark.sql.functions import col, count, date_format, hour

    df_u = (
        df.filter(col("custom_track") == "unqualified")
          .na.fill({"job_id": "0", "publisher_id": "0", "group_id": "0", "campaign_id": "0"})
          .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
          .withColumn("hour", hour(col("ts")))
    )

    agg = (
        df_u.groupBy("job_id", "date", "hour", "publisher_id", "campaign_id", "group_id")
            .agg(count("*").alias("unqualified"))
    )
    return agg

def process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output):

    final_data = (
        clicks_output
        .join(conversion_output, ["job_id", "date", "hour", "publisher_id", "campaign_id", "group_id"], "full_outer")
        .join(qualified_output,  ["job_id", "date", "hour", "publisher_id", "campaign_id", "group_id"], "full_outer")
        .join(unqualified_output, ["job_id", "date", "hour", "publisher_id", "campaign_id", "group_id"], "full_outer")
    )
    return final_data

def retrieve_company_data(spark, jdbc_url: str, driver: str, user: str, password: str):
    sql = "(SELECT job_id, company_id, group_id, campaign_id FROM job_table) AS job_tbl"
    return (
        spark.read.format("jdbc")
        .options(url=jdbc_url, driver=driver, dbtable=sql, user=user, password=password)
        .load()
    )


# --------------DAG Definition---------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    dag_id="etl_cassandra_to_mysql_concrete",
    default_args=default_args,
    start_date=dt.datetime(2025, 10, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["spark", "cassandra", "mysql", "etl"],
) as dag:

    @task
    def get_mysql_latest_time() -> str:
        """Return latest loaded timestamp (string) or a safe default."""
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, schema=MYSQL_DB)
        records = hook.get_records(sql=f"SELECT MAX(updated_at  ) FROM {MYSQL_EVENTS_TABLE}")
        mysql_time: Optional[dt.datetime] = records[0][0] if records and records[0] else None
        return (mysql_time.strftime("%Y-%m-%d %H:%M:%S") if mysql_time else "1998-01-01 23:59:59")

    @task
    def run_etl(mysql_latest_str: str) -> str:
        from pyspark.sql.functions import col, lit, to_timestamp, max as _max, current_timestamp, window

        spark = build_spark_session()

        # Build JDBC URL + creds from Airflow connection
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, schema=MYSQL_DB)
        conn = hook.get_connection(MYSQL_CONN_ID)
        jdbc_url = f"jdbc:mysql://{conn.host}:{conn.port}/{MYSQL_DB}"
        jdbc_user = conn.login
        jdbc_password = conn.password
        jdbc_jobsdb_url = f"jdbc:mysql://{conn.host}:{conn.port}/jobsdb"


        mysql_latest_ts = to_timestamp(lit(mysql_latest_str), "yyyy-MM-dd HH:mm:ss")

        df = (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
            .load()
            .where(col("ts") > mysql_latest_ts)     #Filter timestamp
        )
        if df.rdd.isEmpty():
            spark.stop()
            return f"No new data found after {mysql_latest_str}."

        df = (
            df.select("create_time", "ts", "job_id", "custom_track", "bid", "campaign_id", "group_id", "publisher_id")
              .filter(col("job_id").isNotNull())
        )

        # -----------------------------
        # Transform (aggregations)
        # -----------------------------
        clicks_output = calculating_clicks(df)
        conversion_output = calculating_conversion(df)
        qualified_output = calculating_qualified(df)
        unqualified_output = calculating_unqualified(df)
        cassandra_output = process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output)

        # -----------------------------
        # Enrich with company data from MySQL
        # -----------------------------
        company = retrieve_company_data(
            spark=spark,
            jdbc_url=jdbc_jobsdb_url,
            driver="com.mysql.cj.jdbc.Driver",
            user=jdbc_user,
            password=jdbc_password,
        )

        final_output = (
            cassandra_output.join(company, "job_id", "left")
            # drop duplicate grouping columns from company if exist
            .drop(company.group_id)
            .drop(company.campaign_id)
        )

        # Normalize/rename columns to match MySQL schema
        final_output = (
            final_output.withColumn("sources", lit("Cassandra"))
                        .withColumnRenamed("date", "dates")
                        .withColumnRenamed("hour", "hours")
                        .withColumnRenamed("unqualified", "disqualified_application")
                        .withColumnRenamed("qualified", "qualified_application")
                        .withColumnRenamed("conversions", "conversion")
                        .withColumn("updated_at", current_timestamp()) 
        )

        # -----------------------------
        # Write: upsert into MySQL via foreachPartition
        # -----------------------------
        def upsert_partition(rows_iter):
            import mysql.connector as _mysql  
            _conn = _mysql.connect(
                host=conn.host,
                port=conn.port,
                user=jdbc_user,
                password=jdbc_password,
                database=MYSQL_DB,
            )
            _cursor = _conn.cursor()

            sql = """
                INSERT INTO events (
                    job_id, dates, hours,
                    publisher_id, campaign_id, group_id,
                    company_id,
                    disqualified_application,
                    qualified_application,
                    conversion, clicks, bid_set, spend_hour,
                    sources, updated_at
                ) VALUES (
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s
                )
                ON DUPLICATE KEY UPDATE
                    disqualified_application = VALUES(disqualified_application),
                    qualified_application    = VALUES(qualified_application),
                    conversion               = VALUES(conversion),
                    clicks                   = VALUES(clicks),
                    bid_set                  = VALUES(bid_set),
                    spend_hour               = VALUES(spend_hour),
                    updated_at               = VALUES(updated_at);
            """

            batch = []
            for r in rows_iter:
                # Some columns may be NULL — provide safe defaults
                batch.append(
                    (
                        getattr(r, "job_id", None),
                        getattr(r, "dates", None),
                        getattr(r, "hours", None),
                        getattr(r, "publisher_id", None),
                        getattr(r, "campaign_id", None),
                        getattr(r, "group_id", None),
                        getattr(r, "company_id", None),
                        getattr(r, "disqualified_application", 0),
                        getattr(r, "qualified_application", 0),
                        getattr(r, "conversion", 0),
                        getattr(r, "clicks", 0),
                        getattr(r, "bid_set", 0.0),
                        getattr(r, "spend_hour", 0.0),
                        getattr(r, "sources", "Cassandra"),
                        getattr(r, "updated_at", None),
                    )
                )

            if batch:
                _cursor.executemany(sql, batch)
                _conn.commit()

            _cursor.close()
            _conn.close()
        print("Final output count:", final_output.count())
        final_output.show(truncate=False) 

        final_output.foreachPartition(upsert_partition)

        # Stop spark and return
        spark.stop()
        return "Imported successfully."

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def done(status_msg: str):
        print(status_msg)

    mysql_latest = get_mysql_latest_time()
    status = run_etl(mysql_latest)
    done(status)
