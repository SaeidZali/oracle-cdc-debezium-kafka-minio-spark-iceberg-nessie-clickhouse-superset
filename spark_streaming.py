from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType
)

# ------------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("oracle-cdc-to-iceberg-stream")
    .config(
        "spark.sql.parquet.enableVectorizedReader",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------
# Database and Iceberg Target Table
# ------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS nessie.oracle_cdc_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.customers (
    id INT,
    name STRING,
    last_updated TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, id))
""")

# ------------------------------------------------------------------
# Optional: Create / Refresh ClickHouse Iceberg View once at startup
# ------------------------------------------------------------------
def create_clickhouse_view():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="clickhouse123"
    )

    client.command("SET allow_experimental_database_iceberg = 1")

    desc = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers")

    location = (
        desc
        .filter("col_name = 'Location'")
        .select("data_type")
        .collect()[0][0]
    )

    print("Iceberg table location:", location)

    ch_location = location.replace(
        "s3://oracle-cdc/",
        "http://minio:9000/oracle-cdc/"
    )

    client.command(f"""
    CREATE OR REPLACE VIEW customers_view AS
    SELECT *
    FROM icebergS3('{ch_location}')
    """)

    print("ClickHouse view customers_view created/refreshed")


create_clickhouse_view()

# ------------------------------------------------------------------
# Debezium CDC Schema
# ------------------------------------------------------------------
cdc_schema = StructType([
    StructField("before", StructType([
        StructField("ID", IntegerType()),
        StructField("NAME", StringType())
    ])),
    StructField("after", StructType([
        StructField("ID", IntegerType()),
        StructField("NAME", StringType())
    ])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType())
])

# ------------------------------------------------------------------
# foreachBatch Function: Optimized with Iceberg MERGE
# ------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    """
    Processes one Spark Structured Streaming micro-batch.

    CDC op meaning:
      c = create
      u = update
      d = delete
      r = snapshot/read, ignored here unless you want initial snapshot support
    """

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty")
        return

    print(f"Batch {batch_id}: processing started")

    # Keep only the latest event per customer id inside this micro-batch.
    window_spec = Window.partitionBy("id").orderBy(col("ts_ms").desc())

    latest_changes = (
        batch_df
        .filter(col("id").isNotNull())
        .filter(col("op").isin("c", "u", "d"))
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            col("id"),
            col("name"),
            col("op"),
            col("ts_ms")
        )
    )

    latest_changes.createOrReplaceTempView("cdc_changes_batch")

    # One atomic Iceberg row-level operation instead of:
    # collect IDs -> DELETE -> INSERT
    spark.sql("""
    MERGE INTO nessie.oracle_cdc_db.customers AS target
    USING cdc_changes_batch AS source
    ON target.id = source.id

    WHEN MATCHED AND source.op = 'd' THEN
        DELETE

    WHEN MATCHED AND source.op = 'u' THEN
        UPDATE SET
            target.name = source.name,
            target.last_updated = current_timestamp()

    WHEN NOT MATCHED AND source.op IN ('c', 'u') THEN
        INSERT (id, name, last_updated)
        VALUES (source.id, source.name, current_timestamp())
    """)

    print(f"Batch {batch_id}: merge completed")


# ------------------------------------------------------------------
# Streaming Source
# ------------------------------------------------------------------
stream_df = (
    spark.readStream
    .schema(cdc_schema)
    .option("maxFilesPerTrigger", 10)
    .parquet("s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS")
)

# ------------------------------------------------------------------
# Flatten Debezium Events
# ------------------------------------------------------------------
cdc_df = (
    stream_df
    .select(
        coalesce(col("after.ID"), col("before.ID")).cast("int").alias("id"),
        coalesce(col("after.NAME"), col("before.NAME")).cast("string").alias("name"),
        col("op").cast("string").alias("op"),
        col("ts_ms").cast("long").alias("ts_ms")
    )
    .filter(col("id").isNotNull())
    .filter(col("op").isNotNull())
)

# ------------------------------------------------------------------
# Start Streaming Query
# ------------------------------------------------------------------
query = (
    cdc_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "s3a://oracle-cdc/checkpoints/customers_merge_v1")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Streaming query started. Query ID: {query.id}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming query...")
    query.stop()
except Exception as e:
    print(f"Streaming query failed: {e}")
    query.stop()
    raise
finally:
    spark.stop()
