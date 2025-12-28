from pyspark.sql.functions import col, lit, date_format, hour, avg, sum as _sum, count

def calculating_clicks(df):
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

def upsert_partition(rows_iter, conn_params, table='events'):
    import mysql.connector as _mysql
    _conn = _mysql.connect(**conn_params)
    cursor = _conn.cursor()
    sql = f"""
        INSERT INTO {table} (
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
            company_id               = VALUES(company_id),
            disqualified_application = VALUES(disqualified_application),
            qualified_application    = VALUES(qualified_application),
            conversion               = VALUES(conversion),
            clicks                   = VALUES(clicks),
            bid_set                  = VALUES(bid_set),
            spend_hour               = VALUES(spend_hour),
            sources                  = VALUES(sources),
            updated_at               = VALUES(updated_at);
    """
    batch = []
    for r in rows_iter:
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
        cursor.executemany(sql, batch)
        _conn.commit()
    cursor.close()
    _conn.close()
