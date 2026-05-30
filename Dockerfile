# Use the official Spark Iceberg image as the base
FROM tabulario/spark-iceberg:latest

# Switch to root to install packages
USER root

# Install the Python package
RUN pip install clickhouse-connect
