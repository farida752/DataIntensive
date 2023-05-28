package org.example;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.json.*;
import java.util.Random;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.google.gson.Gson;

public class WeatherMsgAdapter {
    public long station_id;
    public long s_no;
    public String battery_status;
    public long status_timestamp;
    public Weather weather;
    private static final Random random = new Random();


    public WeatherMsgAdapter(){}
    public WeatherMsgAdapter(long stationId, long sNo) throws URISyntaxException, IOException {
        this.station_id = stationId;
        this.s_no = sNo;
        this.battery_status = this.randomBatteryStatus();
        this.status_timestamp = System.currentTimeMillis();
        this.weather = new Weather();
    }

    public String toJsonString() {
        Gson gson = new Gson();
        System.out.println(gson.toJson(this));
        return gson.toJson(this);
    }
    public String getMsg()  {
        return this.toJsonString();
    }

    private static String randomBatteryStatus() {
        int index = random.nextInt(100);
        if (index < 30)
            return "low";
        else if (index < 70)
            return "medium";
        else
            return "high";
    }


    private static class Weather {
        public int humidity;
        public double temperature;
        public double windSpeed;

    private static final String API_KEY = "your_api_key_here";
    private static final String ENDPOINT_URL = "https://api.open-meteo.com/v1/forecast?latitude=31.2001&longitude=29.9187&hourly=relativehumidity_2m&current_weather=true";
        public Weather() throws URISyntaxException, IOException {
//            this.humidity = random.nextInt(101);
//            this.temperature = random.nextInt(101);
//            this.windSpeed = random.nextInt(101);
            CloseableHttpClient httpClient = HttpClients.createDefault();
            // Create an HTTP GET request with the API endpoint URL and your API key
            HttpGet httpGet = new HttpGet(new URI(ENDPOINT_URL + "&apikey=" + API_KEY));

            // Execute the request and get the response
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

            // Process the response as needed
            HttpEntity entity = httpResponse.getEntity();
            String responseString = EntityUtils.toString(entity);
            JSONObject jsonObject = new JSONObject(responseString);
            this.temperature = jsonObject.getJSONObject("current_weather").getDouble("temperature");
            this.windSpeed = jsonObject.getJSONObject("current_weather").getDouble("windspeed");
             String timeStamp = jsonObject.getJSONObject("current_weather").getString("time");
            JSONArray allHours = jsonObject.getJSONObject("hourly").getJSONArray("time");
            int timeIndex = findInArray(timeStamp,allHours);
            this.humidity = jsonObject.getJSONObject("hourly").getJSONArray("relativehumidity_2m").getInt(timeIndex);
            // Close the HTTP client and wait for 10 minutes before making another request
            httpResponse.close();
            httpClient.close();
        }

        public String toJsonString() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }

        public int findInArray(String target , JSONArray array){
            int index = -1;
            for (int i = 0; i < array.length(); i++) {
                if (array.getString(i).equals(target)) {
                    index = i;
                    break;
                }
            }
            return index;
        }

//    public static void main(String[] args) throws URISyntaxException, IOException {
//        Weather w = new Weather();
//    }
    }
}