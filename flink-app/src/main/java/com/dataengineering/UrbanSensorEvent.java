package com.dataengineering;

import java.io.Serializable;

public class UrbanSensorEvent implements Serializable {

    private String sensorId;
    private double temperature;
    private double humidity;
    private int airQualityIndex;
    private long timestamp;

    public UrbanSensorEvent() {
    }

    public UrbanSensorEvent(
            String sensorId,
            double temperature,
            double humidity,
            int airQualityIndex,
            long timestamp) {

        this.sensorId = sensorId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.airQualityIndex = airQualityIndex;
        this.timestamp = timestamp;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public int getAirQualityIndex() {
        return airQualityIndex;
    }

    public void setAirQualityIndex(int airQualityIndex) {
        this.airQualityIndex = airQualityIndex;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UrbanSensorEvent{" +
                "sensorId='" + sensorId + '\'' +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", airQualityIndex=" + airQualityIndex +
                ", timestamp=" + timestamp +
                '}';
    }
}