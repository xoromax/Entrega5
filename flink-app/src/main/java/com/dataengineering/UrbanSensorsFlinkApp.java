package com.dataengineering;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

public class UrbanSensorsFlinkApp {

    private static final String DEFAULT_STREAM_NAME = "dev-stream";
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String DEFAULT_AWS_ACCOUNT_ID = "032619915567";
    private static final String DEFAULT_ICEBERG_WAREHOUSE =
            "s3://lakehouse-maxjaida-dev-032619915567/warehouse/";
    private static final String DEFAULT_GLUE_DATABASE = "lakehouse_db";
    private static final String DEFAULT_ICEBERG_TABLE =
            "urban_sensor_metrics";

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        String streamName = parameters.get(
                "stream-name",
                getEnvironmentVariable(
                        "KINESIS_STREAM_NAME",
                        DEFAULT_STREAM_NAME
                )
        );

        String awsRegion = parameters.get(
                "aws-region",
                getEnvironmentVariable(
                        "AWS_REGION",
                        DEFAULT_AWS_REGION
                )
        );

        String awsAccountId = parameters.get(
                "aws-account-id",
                getEnvironmentVariable(
                        "AWS_ACCOUNT_ID",
                        DEFAULT_AWS_ACCOUNT_ID
                )
        );

        String icebergWarehouse = parameters.get(
                "iceberg-warehouse",
                getEnvironmentVariable(
                        "ICEBERG_WAREHOUSE",
                        DEFAULT_ICEBERG_WAREHOUSE
                )
        );

        String glueDatabase = parameters.get(
                "glue-database",
                getEnvironmentVariable(
                        "GLUE_DATABASE",
                        DEFAULT_GLUE_DATABASE
                )
        );

        String icebergTable = parameters.get(
                "iceberg-table",
                getEnvironmentVariable(
                        "ICEBERG_TABLE",
                        DEFAULT_ICEBERG_TABLE
                )
        );

        String streamArn = String.format(
                "arn:aws:kinesis:%s:%s:stream/%s",
                awsRegion,
                awsAccountId,
                streamName
        );

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.enableCheckpointing(60000L);

        environment
                .getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(30000L);

        environment
                .getCheckpointConfig()
                .setCheckpointTimeout(120000L);

        environment
                .getCheckpointConfig()
                .setMaxConcurrentCheckpoints(1);

        Configuration sourceConfiguration = new Configuration();

        sourceConfiguration.set(
                KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
                KinesisSourceConfigOptions.InitialPosition.LATEST
        );

        KinesisStreamsSource<UrbanSensorEvent> kinesisSource =
                KinesisStreamsSource
                        .<UrbanSensorEvent>builder()
                        .setStreamArn(streamArn)
                        .setSourceConfig(sourceConfiguration)
                        .setDeserializationSchema(
                                new UrbanSensorDeserializationSchema()
                        )
                        .build();

        WatermarkStrategy<UrbanSensorEvent> watermarkStrategy =
                WatermarkStrategy
                        .<UrbanSensorEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10)
                        )
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner
                                        <UrbanSensorEvent>() {

                                    @Override
                                    public long extractTimestamp(
                                            UrbanSensorEvent event,
                                            long recordTimestamp) {

                                        return event.getTimestamp();
                                    }
                                }
                        )
                        .withIdleness(Duration.ofSeconds(30));

        DataStream<UrbanSensorEvent> events =
                environment
                        .fromSource(
                                kinesisSource,
                                watermarkStrategy,
                                "Kinesis Urban Sensors Source"
                        )
                        .returns(
                                TypeInformation.of(
                                        UrbanSensorEvent.class
                                )
                        );

        DataStream<SensorWindowResult> windowResults =
                events
                        .keyBy(UrbanSensorEvent::getSensorId)
                        .window(
                                TumblingEventTimeWindows.of(
                                        Time.minutes(1)
                                )
                        )
                        .aggregate(new SensorMetricsAggregate());

        DataStream<SensorWindowResult> statefulResults =
                windowResults
                        .keyBy(SensorWindowResult::getSensorId)
                        .map(new StatefulResultCounter())
                        .name("Stateful Window Counter")
                        .uid("stateful-window-counter");

        IcebergLakehouseSink.attach(
                statefulResults,
                icebergWarehouse,
                glueDatabase,
                icebergTable,
                awsRegion
        );

        statefulResults
                .print()
                .name("Urban Sensor Results")
                .uid("urban-sensor-results");

        environment.execute(
                "Urban Sensors Stateful Processing"
        );
    }

    private static String getEnvironmentVariable(
            String variableName,
            String defaultValue) {

        String value = System.getenv(variableName);

        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value;
    }

    public static class UrbanSensorDeserializationSchema
            implements DeserializationSchema<UrbanSensorEvent> {

        private static final ObjectMapper OBJECT_MAPPER =
                new ObjectMapper();

        @Override
        public UrbanSensorEvent deserialize(byte[] message) {

            String json = new String(
                    message,
                    StandardCharsets.UTF_8
            );

            try {
                JsonNode node = OBJECT_MAPPER.readTree(json);

                String sensorId =
                        node.path("sensor_id").asText();

                if (sensorId == null || sensorId.trim().isEmpty()) {
                    System.err.println(
                            "Skipping invalid urban sensor event: " + json
                    );
                    return null;
                }

                double temperature =
                        node.path("temperature").asDouble();

                double humidity =
                        node.path("humidity").asDouble();

                int airQualityIndex =
                        node.path("air_quality_index").asInt();

                String timestampValue =
                        node.path("timestamp").asText();

                long eventTimestamp =
                        parseTimestamp(timestampValue);

                return new UrbanSensorEvent(
                        sensorId,
                        temperature,
                        humidity,
                        airQualityIndex,
                        eventTimestamp
                );

            } catch (Exception exception) {
                System.err.println(
                        "Skipping malformed urban sensor event: "
                                + json
                                + ". Cause: "
                                + exception.getMessage()
                );

                return null;
            }
        }

        private long parseTimestamp(String timestampValue) {

            if (timestampValue == null
                    || timestampValue.trim().isEmpty()) {

                return System.currentTimeMillis();
            }

            try {
                return Instant
                        .parse(timestampValue)
                        .toEpochMilli();

            } catch (Exception ignored) {
                return System.currentTimeMillis();
            }
        }

        @Override
        public boolean isEndOfStream(
                UrbanSensorEvent nextElement) {

            return false;
        }

        @Override
        public TypeInformation<UrbanSensorEvent> getProducedType() {

            return TypeInformation.of(
                    UrbanSensorEvent.class
            );
        }
    }

    public static class SensorAccumulator {

        private String sensorId;
        private double temperatureSum;
        private long airQualitySum;
        private long eventCount;

        public SensorAccumulator() {
        }

        public String getSensorId() {
            return sensorId;
        }

        public void setSensorId(String sensorId) {
            this.sensorId = sensorId;
        }

        public double getTemperatureSum() {
            return temperatureSum;
        }

        public void setTemperatureSum(
                double temperatureSum) {

            this.temperatureSum = temperatureSum;
        }

        public long getAirQualitySum() {
            return airQualitySum;
        }

        public void setAirQualitySum(
                long airQualitySum) {

            this.airQualitySum = airQualitySum;
        }

        public long getEventCount() {
            return eventCount;
        }

        public void setEventCount(long eventCount) {
            this.eventCount = eventCount;
        }
    }

    public static class SensorMetricsAggregate
            implements AggregateFunction<
                    UrbanSensorEvent,
                    SensorAccumulator,
                    SensorWindowResult> {

        @Override
        public SensorAccumulator createAccumulator() {
            return new SensorAccumulator();
        }

        @Override
        public SensorAccumulator add(
                UrbanSensorEvent event,
                SensorAccumulator accumulator) {

            accumulator.setSensorId(
                    event.getSensorId()
            );

            accumulator.setTemperatureSum(
                    accumulator.getTemperatureSum()
                            + event.getTemperature()
            );

            accumulator.setAirQualitySum(
                    accumulator.getAirQualitySum()
                            + event.getAirQualityIndex()
            );

            accumulator.setEventCount(
                    accumulator.getEventCount() + 1L
            );

            return accumulator;
        }

        @Override
        public SensorWindowResult getResult(
                SensorAccumulator accumulator) {

            long eventCount =
                    accumulator.getEventCount();

            double averageTemperature =
                    eventCount == 0L
                            ? 0.0
                            : accumulator.getTemperatureSum()
                            / eventCount;

            double averageAirQuality =
                    eventCount == 0L
                            ? 0.0
                            : (double) accumulator
                            .getAirQualitySum()
                            / eventCount;

            return new SensorWindowResult(
                    accumulator.getSensorId(),
                    averageTemperature,
                    averageAirQuality,
                    eventCount,
                    0L
            );
        }

        @Override
        public SensorAccumulator merge(
                SensorAccumulator first,
                SensorAccumulator second) {

            SensorAccumulator merged =
                    new SensorAccumulator();

            merged.setSensorId(
                    first.getSensorId() != null
                            ? first.getSensorId()
                            : second.getSensorId()
            );

            merged.setTemperatureSum(
                    first.getTemperatureSum()
                            + second.getTemperatureSum()
            );

            merged.setAirQualitySum(
                    first.getAirQualitySum()
                            + second.getAirQualitySum()
            );

            merged.setEventCount(
                    first.getEventCount()
                            + second.getEventCount()
            );

            return merged;
        }
    }

    public static class StatefulResultCounter
            extends RichMapFunction<
                    SensorWindowResult,
                    SensorWindowResult> {

        private transient ValueState<Long> processedWindows;

        @Override
        public void open(Configuration parameters) {

            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>(
                            "processed-windows-by-sensor",
                            Long.class
                    );

            processedWindows =
                    getRuntimeContext().getState(descriptor);
        }

        @Override
        public SensorWindowResult map(
                SensorWindowResult result)
                throws Exception {

            Long currentCount =
                    processedWindows.value();

            if (currentCount == null) {
                currentCount = 0L;
            }

            long updatedCount = currentCount + 1L;

            processedWindows.update(updatedCount);

            result.setProcessedWindowCount(
                    updatedCount
            );

            return result;
        }
    }

    public static class SensorWindowResult {

        private String sensorId;
        private double averageTemperature;
        private double averageAirQuality;
        private long eventCount;
        private long processedWindowCount;

        public SensorWindowResult() {
        }

        public SensorWindowResult(
                String sensorId,
                double averageTemperature,
                double averageAirQuality,
                long eventCount,
                long processedWindowCount) {

            this.sensorId = sensorId;
            this.averageTemperature =
                    averageTemperature;
            this.averageAirQuality =
                    averageAirQuality;
            this.eventCount = eventCount;
            this.processedWindowCount =
                    processedWindowCount;
        }

        public String getSensorId() {
            return sensorId;
        }

        public void setSensorId(String sensorId) {
            this.sensorId = sensorId;
        }

        public double getAverageTemperature() {
            return averageTemperature;
        }

        public void setAverageTemperature(
                double averageTemperature) {

            this.averageTemperature =
                    averageTemperature;
        }

        public double getAverageAirQuality() {
            return averageAirQuality;
        }

        public void setAverageAirQuality(
                double averageAirQuality) {

            this.averageAirQuality =
                    averageAirQuality;
        }

        public long getEventCount() {
            return eventCount;
        }

        public void setEventCount(long eventCount) {
            this.eventCount = eventCount;
        }

        public long getProcessedWindowCount() {
            return processedWindowCount;
        }

        public void setProcessedWindowCount(
                long processedWindowCount) {

            this.processedWindowCount =
                    processedWindowCount;
        }

        @Override
        public String toString() {

            return "SensorWindowResult{" +
                    "sensorId='" + sensorId + '\'' +
                    ", averageTemperature=" +
                    averageTemperature +
                    ", averageAirQuality=" +
                    averageAirQuality +
                    ", eventCount=" +
                    eventCount +
                    ", processedWindowCount=" +
                    processedWindowCount +
                    '}';
        }
    }
}
