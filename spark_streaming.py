from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col,
coalesce,
row_number,
current_timestamp,
max as spark_max,
when,
lit
)
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
.appName("iceberg-cdc-stream")
.config(
"spark.sql.extensions",
"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
.config(
"spark.sql.catalog.nessie",
"org.apache.iceberg.spark.SparkCatalog"
)
.config(
"spark.sql.catalog.nessie.catalog-impl",
"org.apache.iceberg.nessie.NessieCatalog"
)
.config(
"spark.sql.catalog.nessie.uri",
"http://nessie:19120/api/v1"
)
.config(
"spark.sql.catalog.nessie.ref",
"main"
)
.config(
"spark.sql.catalog.nessie.io-impl",
"org.apache.iceberg.aws.s3.S3FileIO"
)
.config(
"spark.hadoop.fs.s3a.endpoint",
"http://minio:9000"
)
.config(
"spark.hadoop.fs.s3a.access.key",
"minioadmin"
)
.config(
"spark.hadoop.fs.s3a.secret.key",
"minioadmin"
)
.config(
"spark.hadoop.fs.s3a.path.style.access",
"true"
)
.config(
"spark.sql.parquet.enableVectorizedReader",
"false"
)
.getOrCreate()
)

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS nessie.oracle_cdc_db")

# ------------------------------------------------------------------
# Iceberg Target Table
# ------------------------------------------------------------------
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
# Create ClickHouse Iceberg View (NEW)
# ------------------------------------------------------------------
import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='clickhouse',   # Service name in Docker network
    port=8123,
    username='default',
    password='clickhouse123'
)

# Enable Iceberg support
client.command("SET allow_experimental_database_iceberg = 1")

# Get Iceberg table location from Spark
desc = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers")
location = (
    desc.filter("col_name = 'Location'")
    .select("data_type")
    .collect()[0][0]
)
print("📍 Table location:", location)

# Convert S3 path to MinIO HTTP URL
ch_location = location.replace("s3://oracle-cdc/", "http://minio:9000/oracle-cdc/")

# Create ClickHouse view over Iceberg table
client.command(f"""
CREATE OR REPLACE VIEW customers_view AS
SELECT * FROM icebergS3(
    '{ch_location}'
)
""")
print("✅ ClickHouse view 'customers_view' created")

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
# Option 1: Use DataFrame API Directly (No SQL MERGE)
# ------------------------------------------------------------------
def process_batch_dataframe_api(batch_df, batch_id):
    """Process using DataFrame operations - NO TEMP VIEWS NEEDED"""
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty")
        return
    
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    # Keep latest event per ID within the batch
    window_spec = Window.partitionBy("id").orderBy(col("ts_ms").desc())
    latest_changes = (
        batch_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    # Split into deletes and upserts
    deletes = latest_changes.filter(col("op") == "d")
    upserts = latest_changes.filter(col("op").isin("c", "u"))
    
    # Handle deletes - collect IDs and delete
    if deletes.count() > 0:
        delete_ids = [row.id for row in deletes.select("id").collect()]
        if delete_ids:
            # Convert list to string for SQL IN clause
            ids_str = ','.join(str(id) for id in delete_ids)
            spark.sql(f"""
                DELETE FROM nessie.oracle_cdc_db.customers 
                WHERE id IN ({ids_str})
            """)
            print(f"Deleted {len(delete_ids)} records")
    
    # Handle upserts using DataFrame API instead of SQL MERGE
    if upserts.count() > 0:
        # Prepare upsert data
        upsert_data = upserts.select(
            col("id"),
            col("name"),
            current_timestamp().alias("last_updated")
        )
        
        # Read existing records that match the incoming IDs
        existing_ids = [row.id for row in upserts.select("id").distinct().collect()]
        
        if existing_ids:
            ids_str = ','.join(str(id) for id in existing_ids)
            existing_df = spark.sql(f"""
                SELECT id, name, last_updated 
                FROM nessie.oracle_cdc_db.customers 
                WHERE id IN ({ids_str})
            """)
            
            # Perform update: remove existing records for these IDs
            if existing_df.count() > 0:
                spark.sql(f"""
                    DELETE FROM nessie.oracle_cdc_db.customers 
                    WHERE id IN ({ids_str})
                """)
        
        # Insert all upsert records (both new and updated)
        upsert_data.write \
            .mode("append") \
            .format("iceberg") \
            .saveAsTable("nessie.oracle_cdc_db.customers")
        
        print(f"Upserted {upserts.count()} records")
    
    # Show final count
    final_count = spark.table("nessie.oracle_cdc_db.customers").count()
    print(f"Batch {batch_id} complete. Total customers: {final_count}")

# ------------------------------------------------------------------
# Option 2: Delta Approach (Most Reliable)
# ------------------------------------------------------------------
def process_batch_delta_approach(batch_df, batch_id):
    """Simple append-only approach with compaction"""
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty")
        return
    
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    # Keep latest event per ID within batch
    window_spec = Window.partitionBy("id").orderBy(col("ts_ms").desc())
    latest_changes = (
        batch_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("processed_time", current_timestamp())
    )
    
    # Create or append to staging table
    latest_changes.write \
        .mode("append") \
        .format("iceberg") \
        .saveAsTable("nessie.oracle_cdc_db.customers_staging")
    
    print(f"Batch {batch_id} appended to staging")
    
    # Periodically compact (every 10 batches)
    if batch_id % 10 == 0 and batch_id > 0:
        print(f"Compacting staging table for batch {batch_id}")
        
        # Get latest state per customer
        latest_state = spark.sql("""
            SELECT id, name, ts_ms, op
            FROM (
                SELECT id, name, ts_ms, op,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) as rn
                FROM nessie.oracle_cdc_db.customers_staging
            ) t
            WHERE rn = 1
        """)
        
        # Apply deletes
        deletes = latest_state.filter(col("op") == "d")
        upserts = latest_state.filter(col("op").isin("c", "u"))
        
        if deletes.count() > 0:
            delete_ids = [row.id for row in deletes.select("id").collect()]
            if delete_ids:
                ids_str = ','.join(str(id) for id in delete_ids)
                spark.sql(f"""
                    DELETE FROM nessie.oracle_cdc_db.customers 
                    WHERE id IN ({ids_str})
                """)
        
        if upserts.count() > 0:
            # Overwrite current table with upserts
            upsert_data = upserts.select("id", "name", current_timestamp().alias("last_updated"))
            upsert_data.write \
                .mode("overwrite") \
                .format("iceberg") \
                .option("overwrite-mode", "dynamic") \
                .saveAsTable("nessie.oracle_cdc_db.customers")
        
        print(f"Compaction complete for batch {batch_id}")

# ------------------------------------------------------------------
# Option 3: Simple Insert with Deduplication on Read (Recommended for Testing)
# ------------------------------------------------------------------
def process_batch_append_only(batch_df, batch_id):
    """Just append everything, deduplicate when reading"""
    
    if batch_df.isEmpty():
        return
    
    print(f"Batch {batch_id}: Appending {batch_df.count()} records")
    
    # Add batch metadata
    enriched_df = batch_df.withColumn("batch_id", lit(batch_id)) \
                         .withColumn("ingestion_time", current_timestamp())
    
    # Append to raw table
    enriched_df.write \
        .mode("append") \
        .format("iceberg") \
        .saveAsTable("nessie.oracle_cdc_db.customers_raw")

# ------------------------------------------------------------------
# Streaming Source
# ------------------------------------------------------------------
stream_df = (
spark.readStream
.schema(cdc_schema)
.option("maxFilesPerTrigger", 5)
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
col("op"),
col("ts_ms").cast("long").alias("ts_ms")
)
.filter(col("id").isNotNull())
.filter(col("op").isNotNull())
)

# ------------------------------------------------------------------
# Start Streaming
# ------------------------------------------------------------------
print("Starting streaming query...")
print("Using DataFrame API approach (no temp views)")

query = (
cdc_df.writeStream
.foreachBatch(process_batch_dataframe_api) # Use this one
.option("checkpointLocation", "s3a://oracle-cdc/checkpoints/customers_v3")
.trigger(processingTime="10 seconds")
.start()
)

print(f"Stream started with ID: {query.id}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping stream...")
    query.stop()
except Exception as e:
    print(f"Error: {e}")
    query.stop()
finally:
    spark.stop()
