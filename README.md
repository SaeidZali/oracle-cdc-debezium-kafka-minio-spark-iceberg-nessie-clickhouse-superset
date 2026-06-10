\# Real-Time Data Platform



This repository provides a real-time data platform based on Oracle CDC, Kafka Connect, MinIO/S3, Apache Iceberg/Nessie, Dremio, ClickHouse, Superset, Airflow, and related services.



The platform is designed to capture changes from Oracle, stream them through Kafka Connect, store them in object storage, and make them available for analytics and dashboarding.



\---



\## Architecture Overview



The platform includes the following main components:



\* \*\*Oracle 19c\*\*: Source database for CDC

\* \*\*Debezium / Kafka Connect\*\*: Change Data Capture and connector management

\* \*\*Kafka\*\*: Event streaming layer

\* \*\*MinIO\*\*: S3-compatible object storage

\* \*\*S3 Sink Connector\*\*: Writes CDC data to MinIO

\* \*\*Nessie\*\*: Catalog/versioning layer

\* \*\*Dremio\*\*: Query engine for Iceberg/Nessie data

\* \*\*ClickHouse\*\*: Analytical database

\* \*\*Superset\*\*: Dashboard and visualization

\* \*\*Airflow\*\*: Workflow orchestration

\* \*\*CloudBeaver\*\*: Database client UI

\* \*\*Marquez / OpenLineage\*\*: Data lineage support



\---



\## Prerequisites



Before running the project, make sure the following are installed on your server:



\* Git

\* Docker

\* Docker Compose

\* curl

\* A Linux-based environment

\* Access to Oracle Container Registry



You must also have an Oracle Container Registry account.



\---



\## Clone the Repository



```bash

git clone https://github.com/SaeidZali/real\_time\_platform.git

cd real\_time\_platform/

```



\---



\## Create Required Directories



Create all required local folders for mounted Docker volumes:



```bash

mkdir -p connectors clickhouse/{data,logs} ./data/kafka oradata miniodata postgres\_data airflow/logs airflow/plugins ./cloudbeaver\_data ./dremio/data ./dremio/logs ./nessie-data ./marquez/postgres ./jars

```



\---



\## Set Permissions



Run the following commands to set the required permissions for Docker volumes:



```bash

sudo chmod -R 755 ./data

sudo chmod -R 777 ./data

sudo chown -R 54321:54321 oradata

chmod 775 oradata

chmod 777 miniodata

sudo chmod 777 ./ -R

chmod 777 ./dremio/data ./dremio/logs

chmod 777 ./nessie-data

```



\---



\## Download OpenLineage Spark JAR



Download the OpenLineage Spark integration JAR:



```bash

curl -L \\

https://repo1.maven.org/maven2/io/openlineage/openlineage-spark\_2.12/1.28.0/openlineage-spark\_2.12-1.28.0.jar \\

\-o jars/openlineage-spark.jar

```



\---



\## Oracle Container Registry Login



Before starting the platform, log in to the Oracle Container Registry.



First, go to:



```text

https://container-registry.oracle.com

```



Log in with your Oracle account, then create a new Auth Token from your account profile.



Then run:



```bash

docker login https://container-registry.oracle.com

```



Use:



\* \*\*Username\*\*: your Oracle account email

\* \*\*Password\*\*: your Oracle Auth Token



\---



\## Start the Platform



Run the following command:



```bash

docker compose up -d

```



Check Oracle container logs:



```bash

docker container logs -f dbz\_oracle19

```



\---



\## Configure Oracle Archive Log Mode



Connect to the Oracle container:



```bash

docker container exec -it -e ORACLE\_SID=ORCLCDB dbz\_oracle19 sqlplus sys as sysdba

```



The password is:



```text

oraclepw

```



Run the following SQL commands:



```sql

ALTER SYSTEM SET db\_recovery\_file\_dest\_size = 30G;

ALTER SYSTEM SET db\_recovery\_file\_dest = '/opt/oracle/oradata/ORCLCDB' scope=spfile;



SHUTDOWN IMMEDIATE;



STARTUP MOUNT;



ALTER DATABASE ARCHIVELOG;



ALTER DATABASE OPEN;



ARCHIVE LOG LIST;

```



\---



\## Enable Supplemental Logging and Create LogMiner Tablespaces



Run the following commands in SQLPlus:



```sql

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;



CREATE TABLESPACE logminer\_tbs

DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer\_tbs.dbf'

SIZE 25M REUSE

AUTOEXTEND ON

MAXSIZE UNLIMITED;



ALTER SESSION SET CONTAINER=ORCLPDB1;



CREATE TABLESPACE logminer\_tbs

DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer\_tbs.dbf'

SIZE 25M REUSE

AUTOEXTEND ON

MAXSIZE UNLIMITED;



EXIT;

```



\---



\## Run Oracle Setup Script



Make the setup script executable:



```bash

chmod +x run\_oracle\_setup.sh

```



Run it:



```bash

./run\_oracle\_setup.sh

```



\---



\## Create MinIO Bucket



Enter the MinIO client container:



```bash

docker container exec -it mc bash

```



Configure the MinIO alias:



```bash

mc alias set myminio http://minio:9000 minioadmin minioadmin

```



Create the bucket:



```bash

mc mb myminio/oracle-cdc

```



Make the bucket public:



```bash

mc anonymous set public myminio/oracle-cdc

```



Exit the container:



```bash

exit

```



\---



\## Register Oracle CDC Connector



Register the Oracle connector with Kafka Connect:



```bash

curl --noproxy localhost,127.0.0.1 -i -X POST \\

&#x20; -H "Accept:application/json" \\

&#x20; -H "Content-Type:application/json" \\

&#x20; localhost:8083/connectors \\

&#x20; -d @register-oracle.json

```



\---



\## Register S3 Sink Connector



Register the S3 Sink connector:



```bash

curl --noproxy '\*' -i -X POST \\

&#x20; -H "Accept:application/json" \\

&#x20; -H "Content-Type:application/json" \\

&#x20; http://localhost:8083/connectors \\

&#x20; -d @s3-sink.json

```



> Note: To create topics and flush data to the sink, you must reach the configured flush size.



\---



\## Airflow



Open the Airflow UI and check the available DAGs.



Typical URL:



```text

http://localhost:8080

```



\---



\## Superset



Open Superset:



```text

http://localhost:8088

```



Connect Superset to ClickHouse and create dashboards based on the analytical data.



\---



\## Dremio Configuration



Open Dremio and add a new source.



Select \*\*Nessie\*\* as the source type.



\### General Tab



Use the following configuration:



```text

Nessie endpoint URL: http://nessie:19120/api/v2

Authentication type: None

```



\### Storage Tab



Use AWS/S3-compatible storage settings:



```text

AWS root path: s3://oracle-cdc

AWS access key: minioadmin

AWS access secret: minioadmin

```



Add the following connection properties:



```text

fs.s3a.path.style.access = true

fs.s3a.endpoint = minio:9000

dremio.s3.compat = true

```



\---



\## Example Dremio Query



You can query data from Nessie using a specific commit:



```sql

SELECT \*

FROM nessie."oracle\_cdc\_db".customers

AT COMMIT "b01a7128b3f452342fc62136ec15eeac8d7b593504cb6bdff80b4fd1b169fab3";

```



\---



\## Useful Commands



Check running containers:



```bash

docker ps

```



Check logs for a container:



```bash

docker logs -f <container\_name>

```



Restart the platform:



```bash

docker compose restart

```



Stop the platform:



```bash

docker compose down

```



Stop and remove volumes:



```bash

docker compose down -v

```



\---



\## Notes



\* Oracle CDC requires Archive Log mode.

\* Supplemental logging must be enabled for Debezium/LogMiner.

\* The Oracle container may take several minutes to become ready.

\* MinIO bucket `oracle-cdc` must exist before the S3 Sink connector writes data.

\* Dremio must be configured with Nessie and MinIO-compatible S3 settings.

\* Superset can be connected to ClickHouse for dashboard creation.

\* Make sure all required directories have correct permissions before running Docker Compose.



\---



\## Default Credentials



| Service    | Username   | Password   |

| ---------- | ---------- | ---------- |

| Oracle SYS | sys        | oraclepw   |

| MinIO      | minioadmin | minioadmin |



Other service credentials may be defined in the project `.env` file or `docker-compose.yml`.



\---



\## Troubleshooting



\### Oracle container is not ready



Check logs:



```bash

docker container logs -f dbz\_oracle19

```



Wait until the database is fully initialized before running SQL commands.



\### Kafka Connect connector registration fails



Check if Kafka Connect is running:



```bash

curl http://localhost:8083/connectors

```



Also verify that `register-oracle.json` and `s3-sink.json` exist in the project root.



\### MinIO bucket does not exist



Create the bucket manually:



```bash

docker container exec -it mc bash

mc alias set myminio http://minio:9000 minioadmin minioadmin

mc mb myminio/oracle-cdc

mc anonymous set public myminio/oracle-cdc

exit

```



\### Dremio cannot access MinIO



Verify the following properties:



```text

fs.s3a.path.style.access = true

fs.s3a.endpoint = minio:9000

dremio.s3.compat = true

```



Also make sure the bucket name is correct:



```text

oracle-cdc

```



