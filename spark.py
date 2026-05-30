from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("iceberg-test") \
    .getOrCreate()
spark.sql("show catalogs").show()
spark.sql("USE nessie")
spark.sql("SELECT current_catalog()").show()
spark.sql("CREATE DATABASE IF NOT EXISTS oracle_cdc_db")
spark.sql("show databases").show()
spark.sql("USE oracle_cdc_db")
spark.sql("DROP TABLE IF EXISTS nessie.oracle_cdc_db.customers")
spark.sql("""
CREATE TABLE nessie.oracle_cdc_db.customers
USING iceberg
AS
WITH all_records AS (
    SELECT
        COALESCE(after.ID, before.ID) AS id,
        COALESCE(after.NAME, before.NAME) AS name,
        op,
        ts_ms,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(after.ID, before.ID)
            ORDER BY ts_ms DESC
        ) AS rn
    FROM parquet.`s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS`
    WHERE COALESCE(after.ID, before.ID) IS NOT NULL
)
SELECT id, name
FROM all_records
WHERE rn = 1 AND op <> 'd'
""")
spark.sql("show tables").show()
spark.sql("SELECT * FROM nessie.oracle_cdc_db.customers").show()
import clickhouse_connect
client = clickhouse_connect.get_client(
    host='clickhouse',   # 👈 NOT localhost
    port=8123,
    username='default',
    password='clickhouse123'
)
client.command("SET allow_experimental_database_iceberg = 1")
client.command("DROP TABLE IF EXISTS customers_table")
desc = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers")
location = (
    desc.filter("col_name = 'Location'")
    .select("data_type")
    .collect()[0][0]
)
print("📍 Table location:", location)
ch_location = location.replace("s3://oracle-cdc/", "http://minio:9000/oracle-cdc/")
client.command(f"""
CREATE TABLE customers_table
ENGINE = Iceberg(
    '{ch_location}'
)
""")
