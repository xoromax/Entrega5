# Aplicación Flink

Aplicación desarrollada en Java utilizando Apache Flink.

## Funcionalidad

- Consume eventos desde Kinesis.
- Procesa métricas urbanas.
- Genera agregaciones por ventana.
- Escribe resultados en Apache Iceberg.
- Utiliza AWS Glue Catalog.

## Componentes principales

- UrbanSensorsFlinkApp.java
- IcebergLakehouseSink.java

## Compilación

mvn clean package

## Artefacto

urban-sensors-flink.jar