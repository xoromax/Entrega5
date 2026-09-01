# Entrega 5 - Urban Sensors Lakehouse con Apache Flink, Apache Iceberg y AWS

## Autor

**Maximiliano Jaida Santander**

---

# 1. Objetivo

Implementar una arquitectura Data Engineering basada en servicios AWS para procesar eventos de sensores urbanos en tiempo real utilizando:

- Amazon Kinesis Data Streams
- Amazon Managed Service for Apache Flink
- Apache Iceberg
- AWS Glue Catalog
- Amazon S3
- Amazon Athena
- Terraform

---

# 2. Arquitectura

                +-------------+
                | Kinesis     |
                | Data Stream |
                +------+------+
                       |
                       v
                +-------------+
                | Apache      |
                | Flink       |
                +------+------+
                       |
                       v
                +-------------+
                | Iceberg     |
                | GlueCatalog |
                +------+------+
                       |
                       v
                +-------------+
                | S3 Lakehouse|
                +------+------+
                       |
                       v
                +-------------+
                | Athena      |
                +-------------+

---

# 3. Infraestructura Implementada

## Kinesis Stream


dev-stream

## Bucket Bronze

bronze-datalake-maxjaida-dev


## Bucket Lakehouse

lakehouse-maxjaida-dev-032619915567


## Glue Database

lakehouse_db

## Tabla Iceberg

urban_sensor_metrics


---

# 4. Despliegue con Terraform

## Validación

Comando:

```powershell
terraform validate
```

### Evidencia 1

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1> terraform -chdir=".\environments\dev" validate
Success! The configuration is valid.

---

## Aplicación Terraform

Comando:

```powershell
terraform apply
```

### Evidencia 2

captura de:

Apply complete! Resources: 1 added, 2 changed, 1 destroyed.

Outputs:

audit_role_arn = "arn:aws:iam::032619915567:role/audit_readonly_role"
bronze_bucket_name = "bronze-datalake-maxjaida-dev"
data_processing_role_arn = "arn:aws:iam::032619915567:role/data_processing_role"
firehose_name = "dev-firehose"
private_subnet_ids = [
  "subnet-08d46ac28bc8d6c20",
  "subnet-0313a910558a49e2d",
]
stream_arn = "arn:aws:kinesis:us-east-1:032619915567:stream/dev-stream"
stream_name = "dev-stream"
vpc_id = "vpc-0a631c0f09fdb40c5"

---

# 5. Construcción del JAR Flink

Comando:

```powershell
mvn clean package
```

### Evidencia 3

captura de:

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  24.638 s
[INFO] Finished at: 2026-08-31T17:08:20-04:00
[INFO] ------------------------------------------------------------------------
---

## Artefacto generado

Comando:

```powershell
Get-ChildItem .\target\urban-sensors-flink.jar
```

### Evidencia 4

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1> Get-ChildItem .\flink-app\target\urban-sensors-flink.jar


    Directory: C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1\flink-app\target


Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
-a----         8/31/2026   5:08 PM      138438359 urban-sensors-flink.jar


---

# 6. Despliegue de Apache Flink

## Estado de la aplicación

Comando:

```powershell
aws kinesisanalyticsv2 describe-application `
--application-name urban-sensors-flink-dev `
--query "ApplicationDetail.[ApplicationStatus,ApplicationVersionId]" `
--output table `
--no-cli-pager
```

### Evidencia 5

captura:

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1> aws kinesisanalyticsv2 describe-application --application-name urban-sensors-flink-dev --query "ApplicationDetail.ApplicationStatus" --output text --no-cli-pager
RUNNING
---

# 7. Creación de la Tabla Iceberg

## Verificación Glue

Comando:

```powershell
aws glue get-table `
--database-name lakehouse_db `
--name urban_sensor_metrics `
--output table `
--no-cli-pager
```

### Evidencia 6

captura:

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1> aws glue get-table --database-name lakehouse_db --name urban_sensor_metrics --query "Table.StorageDescriptor.Columns[*].[Name,Type]" --output table --no-cli-pager
-----------------------------------------
|               GetTable                |
+-------------------------+-------------+
|  sensor_id              |  string     |
|  average_temperature    |  double     |
|  average_air_quality    |  double     |
|  event_count            |  bigint     |
|  processed_window_count |  bigint     |
|  processed_at           |  timestamp  |
|  event_date             |  date       |
+-------------------------+-------------+

---

## Esquema de la tabla

Comando:

```powershell
aws glue get-table `
--database-name lakehouse_db `
--name urban_sensor_metrics `
--query "Table.StorageDescriptor.Columns[*].[Name,Type]" `
--output table `
--no-cli-pager
```

### Evidencia 7

captura:

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1> aws glue get-table --database-name lakehouse_db --name urban_sensor_metrics --query "Table.StorageDescriptor.Columns[*].[Name,Type]" --output table --no-cli-pager
-----------------------------------------
|               GetTable                |
+-------------------------+-------------+
|  sensor_id              |  string     |
|  average_temperature    |  double     |
|  average_air_quality    |  double     |
|  event_count            |  bigint     |
|  processed_window_count |  bigint     |
|  processed_at           |  timestamp  |
|  event_date             |  date       |
+-------------------------+-------------+

---

# 8. Publicación de Eventos en Kinesis

Se enviaron eventos simulados para cinco sensores.

Ejemplo:

```json
{
  "sensor_id":"sensor_1",
  "temperature":21.5,
  "humidity":55.2,
  "air_quality_index":42
}
```

### Evidencia 8

captura mostrando los cinco comandos con:

{
    "ShardId": "shardId-000000000000",
    "SequenceNumber": "49677820014625946248792971310018783580295765732326637570",
    "EncryptionType": "KMS"
}
Enviando: {"timestamp":"2026-08-31T21:25:54.300Z","sensor_id":"sensor_2","air_quality_index":58,"humidity":61.4,"temperature":24.8}
{
    "ShardId": "shardId-000000000000",
    "SequenceNumber": "49677820014625946248792971310019992506115380705098727426",
    "EncryptionType": "KMS"
}
Enviando: {"timestamp":"2026-08-31T21:25:59.196Z","sensor_id":"sensor_3","air_quality_index":73,"humidity":48.7,"temperature":27.1}
{
    "ShardId": "shardId-000000000000",
    "SequenceNumber": "49677820014625946248792971310021201431934995677870817282",
    "EncryptionType": "KMS"
}
Enviando: {"timestamp":"2026-08-31T21:26:04.370Z","sensor_id":"sensor_4","air_quality_index":35,"humidity":69.3,"temperature":19.6}
{
    "ShardId": "shardId-000000000000",
    "SequenceNumber": "49677820014625946248792971310022410357754610650642907138",
    "EncryptionType": "KMS"
}
Enviando: {"timestamp":"2026-08-31T21:26:09.393Z","sensor_id":"sensor_5","air_quality_index":81,"humidity":44.9,"temperature":30.2}
{
    "ShardId": "shardId-000000000001",
    "SequenceNumber": "49677820014648246993991501933165155001846873984920977426",
    "EncryptionType": "KMS"


# 9. Verificación de Checkpoints Flink

Comando:

```powershell
aws logs tail "/aws/kinesis-analytics/urban-sensors-flink-dev" `
--log-stream-names application `
--since 10m `
--format short `
--no-cli-pager |
Select-String "Completed checkpoint"
```

### Evidencia 9

📷 Insertar captura mostrando:

2026-08-31T21:23:02 {"applicationARN":"arn:aws:kinesisanalytics:us-east-1:032619915567:application/urban-sensors-flink-
dev","applicationVersionId":"8","locationInformation":"org.apache.iceberg.flink.sink.IcebergFilesCommitter.notifyCheckp
ointComplete(IcebergFilesCommitter.java:241)","logger":"org.apache.iceberg.flink.sink.IcebergFilesCommitter","message":
"Checkpoint 5 completed.

2026-08-31T21:24:02 {"applicationARN":"arn:aws:kinesisanalytics:us-east-1:032619915567:application/urban-sensors-flink-
dev","applicationVersionId":"8","locationInformation":"org.apache.iceberg.flink.sink.IcebergFilesCommitter.notifyCheckp
ointComplete(IcebergFilesCommitter.java:241)","logger":"org.apache.iceberg.flink.sink.IcebergFilesCommitter","message":
"Checkpoint 6 completed

---

# 10. Verificación de Snapshots Iceberg

Comando:

```powershell
aws s3 ls s3://lakehouse-maxjaida-dev-032619915567/warehouse/urban_sensor_metrics/ --recursive
```

### Evidencia 10

captura mostrando:

PS C:\Users\jaidasan\OneDrive - TomTom\Documents\Curso Data Engineering\Entrega1> aws s3 ls s3://lakehouse-maxjaida-dev-032619915567/warehouse/urban_sensor_metrics/ --recursive
2026-08-31 14:50:15       1264 warehouse/urban_sensor_metrics/metadata/00000-44ce43d4-99e9-4418-ae72-588551103a91.metadata.json
2026-08-31 17:28:03       3136 warehouse/urban_sensor_metrics/metadata/00001-27ea3eb4-ed94-46f4-b9ed-1ddba5b5afe6.metadata.json
2026-08-31 17:38:03       4413 warehouse/urban_sensor_metrics/metadata/00002-e34c0c6e-6747-4809-9835-6ffcf7173ec5.metadata.json
2026-08-31 17:48:03       5690 warehouse/urban_sensor_metrics/metadata/00003-28f63264-b615-40d5-81ee-a656ba67233e.metadata.json
---

# 11. Verificación de Commits Iceberg

Comando:

```powershell
aws logs tail "/aws/kinesis-analytics/urban-sensors-flink-dev" `
--log-stream-names application `
--since 15m `
--format short `
--no-cli-pager |
Select-String "Committed snapshot|Successfully committed"
```

### Evidencia 11

📷 Insertar captura de:

2026-09-01T13:38:02 {"applicationARN":"arn:aws:kinesisanalytics:us-east-1:032619915567:application/urban-sensors-flink-dev","applicationVersionI
d":"8","locationInformation":"org.apache.iceberg.BaseMetastoreTableOperations.commit(BaseMetastoreTableOperations.java:132)","logger":"org.apach
e.iceberg.BaseMetastoreTableOperations","message":"Successfully committed to table urban_sensors_glue_catalog.lakehouse_db.urban_sensor_metrics 
in 242 ms","messageSchemaVersion":"1","messageType":"INFO","threadName":"urban-sensor-metrics-IcebergFilesCommitter -\u003e Sink: 
urban-sensor-metrics-IcebergSink urban_sensors_glue_catalog.lakehouse_db.urban_sensor_metrics (1/1)#0"}
2026-09-01T13:38:02 {"applicationARN":"arn:aws:kinesisanalytics:us-east-1:032619915567:application/urban-sensors-flink-dev","applicationVersionI
d":"8","locationInformation":"org.apache.iceberg.SnapshotProducer.commit(SnapshotProducer.java:426)","logger":"org.apache.iceberg.SnapshotProduc
er","message":"Committed snapshot 2432492304920140681 
(MergeAppend)","messageSchemaVersion":"1","messageType":"INFO","threadName":"urban-sensor-metrics-IcebergFilesCommitter -\u003e Sink: 
urban-sensor-metrics-IcebergSink urban_sensors_glue_catalog.lakehouse_db.urban_sensor_metrics (1/1)#0"}

---

### Evidencia 12

captura de:

"message":"Committed snapshot 2432492304920140681 

---

# 12. Evolución de Metadata Iceberg

### Evidencia 13

captura:

```text
Refreshing table metadata from new version

metadata/00001-27ea3eb4-ed94-46f4-b9ed-1ddba5b5afe6.metadata.json
```

---

# 13. Athena

Consulta ejecutada:

```sql
SELECT *
FROM lakehouse_db.urban_sensor_metrics
LIMIT 20;
```



# 14. Problemas Encontrados y Soluciones

## Problema 1

```text
NoClassDefFoundError:
org/apache/hadoop/conf/Configuration
```

### Solución

Agregar dependencia Hadoop mínima al artefacto.

---

## Problema 2

```text
NoClassDefFoundError:
com/ctc/wstx/io/InputBootstrapper
```

### Solución

Agregar:

- woodstox-core
- stax2-api

---

## Problema 3

```text
NoSuchMethodError:
CommittableSummary
```

### Solución

Migrar desde:

```text
Iceberg 1.10.2
```

a:

```text
Iceberg 1.6.1
```

compatible con AWS Managed Flink.

---

# 15. Resultados

Se logró implementar exitosamente una arquitectura Lakehouse basada en Apache Iceberg utilizando AWS.

Validaciones completadas:

- ✅ Terraform desplegado correctamente.
- ✅ Kinesis Stream operativo.
- ✅ Apache Flink ejecutándose.
- ✅ Glue Catalog configurado.
- ✅ Tabla Iceberg creada.
- ✅ Metadata Iceberg generada.
- ✅ Checkpoints completados.
- ✅ Snapshots Iceberg generados.
- ✅ Commits exitosos hacia Glue Catalog.
- ✅ Integración end-to-end validada.

---


