from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("oracle-cdc-iceberg")
    .getOrCreate()
)

# ------------------------------------------------------------------
# Catalog
# ------------------------------------------------------------------

spark.sql("USE nessie")
spark.sql("CREATE DATABASE IF NOT EXISTS oracle_cdc_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.customers (
    id BIGINT,
    name STRING
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.cdc_watermark (
    table_name STRING,
    last_ts BIGINT
)
USING iceberg
""")

# ------------------------------------------------------------------
# Get current watermark
# ------------------------------------------------------------------

watermark_exists = spark.sql("""
SELECT COUNT(*) cnt
FROM nessie.oracle_cdc_db.cdc_watermark
WHERE table_name='customers'
""").first()["cnt"]

if watermark_exists == 0:
    spark.sql("""
    INSERT INTO nessie.oracle_cdc_db.cdc_watermark
    VALUES ('customers',0)
    """)

last_ts = spark.sql("""
SELECT last_ts
FROM nessie.oracle_cdc_db.cdc_watermark
WHERE table_name='customers'
""").first()["last_ts"]

print(f"Current watermark: {last_ts}")

# ------------------------------------------------------------------
# Read only new CDC events
# ------------------------------------------------------------------

cdc_df = spark.sql(f"""
WITH cdc AS (
    SELECT
        COALESCE(after.ID,before.ID) AS id,
        COALESCE(after.NAME,before.NAME) AS name,
        op,
        ts_ms
    FROM parquet.`s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS`
    WHERE COALESCE(after.ID,before.ID) IS NOT NULL
      AND ts_ms > {last_ts}
),

latest_per_id AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY ts_ms DESC
            ) rn
        FROM cdc
    )
    WHERE rn = 1
)

SELECT
    id,
    name,
    op,
    ts_ms
FROM latest_per_id
""")

if cdc_df.count() == 0:
    print("No new CDC records.")
    spark.stop()
    raise SystemExit()

cdc_df.createOrReplaceTempView("cdc_changes")

print("CDC rows to apply:")
spark.sql("""
SELECT *
FROM cdc_changes
ORDER BY id
""").show(truncate=False)

# ------------------------------------------------------------------
# Delete existing versions
# ------------------------------------------------------------------

spark.sql("""
DELETE FROM nessie.oracle_cdc_db.customers
WHERE id IN (
    SELECT DISTINCT id
    FROM cdc_changes
)
""")

# ------------------------------------------------------------------
# Insert current version
# ------------------------------------------------------------------

spark.sql("""
INSERT INTO nessie.oracle_cdc_db.customers
SELECT
    id,
    name
FROM cdc_changes
WHERE op IN ('c','u','r')
""")

# ------------------------------------------------------------------
# Update watermark AFTER successful load
# ------------------------------------------------------------------

new_watermark = cdc_df.agg({"ts_ms": "max"}).first()[0]

spark.sql(f"""
UPDATE nessie.oracle_cdc_db.cdc_watermark
SET last_ts = {new_watermark}
WHERE table_name='customers'
""")

print(f"New watermark: {new_watermark}")

# ------------------------------------------------------------------
# Verify
# ------------------------------------------------------------------

print("Customers table:")

spark.sql("""
SELECT *
FROM nessie.oracle_cdc_db.customers
ORDER BY id
""").show(truncate=False)

print("Duplicate check:")

spark.sql("""
SELECT
    id,
    COUNT(*) cnt
FROM nessie.oracle_cdc_db.customers
GROUP BY id
HAVING COUNT(*) > 1
""").show(truncate=False)

# ------------------------------------------------------------------
# ClickHouse view
# ------------------------------------------------------------------

import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="clickhouse123"
)

client.command("SET allow_experimental_database_iceberg = 1")

desc = spark.sql("""
DESCRIBE EXTENDED nessie.oracle_cdc_db.customers
""")

location = (
    desc
    .filter("col_name = 'Location'")
    .select("data_type")
    .collect()[0][0]
)

print("Iceberg location:", location)

ch_location = location.replace(
    "s3://oracle-cdc/",
    "http://minio:9000/oracle-cdc/"
)

client.command("DROP VIEW IF EXISTS customers_view")

client.command(f"""
CREATE VIEW customers_view AS
SELECT *
FROM icebergS3('{ch_location}')
""")

print("ClickHouse view recreated.")
