package com.abhishek.trending;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class KafkaMeetUpProducer {
    String topic = "meetup-trending-topics";

    public Producer<String, String>  initialiseProducer(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.248.57:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("partition.assignment.strategy", "range");

        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer ;
    }

    public Producer getProducer(){
      return initialiseProducer();
    }
    public static void main(String[] args) {
        //KafkaMeetUpProducer pr = new KafkaMeetUpProducer();
        try {
            new KafkaMeetUpProducer().readAndPushTopics();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  void readAndPushTopics() throws Exception {
        sendGet("https://stream.meetup.com/2/rsvps");
    }

    private void sendGet(String url) throws Exception {

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        Producer<String, String>  pr = getProducer();
        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        //con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();


        while ((inputLine = in.readLine()) != null) {
            System.out.println(inputLine);
            pr.send(new ProducerRecord(topic , inputLine));
        }
        in.close();

        //print result

    }
}
