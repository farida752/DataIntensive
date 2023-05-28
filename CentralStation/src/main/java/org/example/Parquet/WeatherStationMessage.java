package org.example.Parquet;

public class WeatherStationMessage {
    private long station_id;
    private long s_no;
    private String battery_status;
    private long status_timestamp;
    private Weather weather;

    class Weather {
        private int humidity;
        private int temperature;
        private int windSpeed;


         public Weather(){}
        public Weather(int humidity, int temperature, int windSpeed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.windSpeed = windSpeed;
        }

        public int getHumidity() {
            return humidity;
        }

        public void setHumidity(int humidity) {
            this.humidity = humidity;
        }

        public int getTemperature() {
            return temperature;
        }

        public void setTemperature(int temperature) {
            this.temperature = temperature;
        }

        public int getWindSpeed() {
            return windSpeed;
        }

        public void setWindSpeed(int windSpeed) {
            this.windSpeed = windSpeed;
        }
    }
    public WeatherStationMessage(){

    }
    public WeatherStationMessage(int stationId, int sequenceNumber, String batteryStatus, long timestamp ,Weather weather) {
        this.station_id = stationId;
        this.s_no = sequenceNumber;
        this.battery_status = batteryStatus;
        this.status_timestamp = timestamp;
        this.weather = weather;
    }

    public long getStation_id() {
        return station_id;
    }

    public void setStation_id(long station_id) {
        this.station_id = station_id;
    }

    public long getS_no() {
        return s_no;
    }

    public void setS_no(long s_no) {
        this.s_no = s_no;
    }

    public String getBattery_status() {
        return battery_status;
    }

    public void setBattery_status(String battery_status) {
        this.battery_status = battery_status;
    }

    public long getStatus_timestamp() {
        return status_timestamp;
    }

    public void setStatus_timestamp(long status_timestamp) {
        this.status_timestamp = status_timestamp;
    }

    public Weather getWeather() {
        return weather;
    }

    public void setWeather(Weather weather) {
        this.weather = weather;
    }
}