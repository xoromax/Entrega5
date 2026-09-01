package com.dataengineering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public final class IcebergLakehouseSink {

    private static final String CATALOG_NAME = "urban_sensors_glue_catalog";
    private static final String GLUE_CATALOG_IMPLEMENTATION =
            "org.apache.iceberg.aws.glue.GlueCatalog";
    private static final String S3_FILE_IO_IMPLEMENTATION =
            "org.apache.iceberg.aws.s3.S3FileIO";

    private IcebergLakehouseSink() {
    }

    public static void attach(
            DataStream<UrbanSensorsFlinkApp.SensorWindowResult> results,
            String warehouseUri,
            String databaseName,
            String tableName,
            String awsRegion) {

        CatalogLoader catalogLoader = createCatalogLoader(
                warehouseUri,
                awsRegion
        );

        TableIdentifier tableIdentifier = TableIdentifier.of(
                Namespace.of(databaseName),
                tableName
        );

        Table table = loadOrCreateTable(
                catalogLoader,
                tableIdentifier
        );

        TableLoader tableLoader = TableLoader.fromCatalog(
                catalogLoader,
                tableIdentifier
        );

        DataStream<RowData> icebergRows = results
                .map(IcebergLakehouseSink::toRowData)
                .returns(TypeInformation.of(RowData.class))
                .name("Map Results to Iceberg RowData")
                .uid("map-results-to-iceberg-rowdata");

        FlinkSink.forRowData(icebergRows)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .uidPrefix("urban-sensor-metrics")
                .append();
    }

    private static CatalogLoader createCatalogLoader(
            String warehouseUri,
            String awsRegion) {

        Map<String, String> catalogProperties = new HashMap<>();

        catalogProperties.put(
                CatalogProperties.WAREHOUSE_LOCATION,
                warehouseUri
        );
        catalogProperties.put(
                CatalogProperties.CATALOG_IMPL,
                GLUE_CATALOG_IMPLEMENTATION
        );
        catalogProperties.put(
                CatalogProperties.FILE_IO_IMPL,
                S3_FILE_IO_IMPLEMENTATION
        );
        catalogProperties.put(
                "client.region",
                awsRegion
        );

        Configuration hadoopConfiguration = new Configuration(false);

        return CatalogLoader.custom(
                CATALOG_NAME,
                catalogProperties,
                hadoopConfiguration,
                GLUE_CATALOG_IMPLEMENTATION
        );
    }

    private static Table loadOrCreateTable(
            CatalogLoader catalogLoader,
            TableIdentifier tableIdentifier) {

        Catalog catalog = catalogLoader.loadCatalog();

        if (catalog.tableExists(tableIdentifier)) {
            return catalog.loadTable(tableIdentifier);
        }

        Schema schema = createIcebergSchema();

        PartitionSpec partitionSpec = PartitionSpec
                .builderFor(schema)
                .identity("event_date")
                .build();

        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", "2");
        tableProperties.put("write.format.default", "parquet");
        tableProperties.put("write.parquet.compression-codec", "snappy");
        tableProperties.put("write.target-file-size-bytes", "134217728");

        return catalog.createTable(
                tableIdentifier,
                schema,
                partitionSpec,
                tableProperties
        );
    }

    private static Schema createIcebergSchema() {
        return new Schema(
                Types.NestedField.required(
                        1,
                        "sensor_id",
                        Types.StringType.get()
                ),
                Types.NestedField.required(
                        2,
                        "average_temperature",
                        Types.DoubleType.get()
                ),
                Types.NestedField.required(
                        3,
                        "average_air_quality",
                        Types.DoubleType.get()
                ),
                Types.NestedField.required(
                        4,
                        "event_count",
                        Types.LongType.get()
                ),
                Types.NestedField.required(
                        5,
                        "processed_window_count",
                        Types.LongType.get()
                ),
                Types.NestedField.required(
                        6,
                        "processed_at",
                        Types.TimestampType.withZone()
                ),
                Types.NestedField.required(
                        7,
                        "event_date",
                        Types.DateType.get()
                )
        );
    }

    private static RowData toRowData(
            UrbanSensorsFlinkApp.SensorWindowResult result) {

        Instant processedAt = Instant.now();
        LocalDate eventDate = processedAt
                .atZone(ZoneOffset.UTC)
                .toLocalDate();

        GenericRowData row = new GenericRowData(7);
        row.setField(0, StringData.fromString(result.getSensorId()));
        row.setField(1, result.getAverageTemperature());
        row.setField(2, result.getAverageAirQuality());
        row.setField(3, result.getEventCount());
        row.setField(4, result.getProcessedWindowCount());
        row.setField(5, TimestampData.fromInstant(processedAt));
        row.setField(6, Math.toIntExact(eventDate.toEpochDay()));

        return row;
    }
}
