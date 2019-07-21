package com.abhishek.trending;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.types.DataTypes.StringType;

class Venue{
    String venue_name;
    double lon;
    double lat;
    long venue_id;

    public String getVenue_name() {
        return venue_name;
    }

    public void setVenue_name(String venue_name) {
        this.venue_name = venue_name;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public long getVenue_id() {
        return venue_id;
    }

    public void setVenue_id(long venue_id) {
        this.venue_id = venue_id;
    }
}
@JsonIgnoreProperties(ignoreUnknown = true)
class  Member{
    long member_id;
    String member_name;
    String photo;
   // String other_services;

//    public String getOther_services() {
//        return other_services;
//    }
//
//    public void setOther_services(String other_services) {
//        this.other_services = other_services;
//    }

    public long getMember_id() {
        return member_id;
    }

    public void setMember_id(long member_id) {
        this.member_id = member_id;
    }

    public String getMember_name() {
        return member_name;
    }

    public void setMember_name(String member_name) {
        this.member_name = member_name;
    }

    public String getPhoto() {
        return photo;
    }

    public void setPhoto(String photo) {
        this.photo = photo;
    }
}
class Event{
    String event_id;
    String event_name;
    String event_url;
    long time;

    public String getEvent_id() {
        return event_id;
    }

    public void setEvent_id(String event_id) {
        this.event_id = event_id;
    }

    public String getEvent_name() {
        return event_name;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public String getEvent_url() {
        return event_url;
    }

    public void setEvent_url(String event_url) {
        this.event_url = event_url;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
class Element{
    String topic_name;
    String urlkey;

    public String getTopic_name() {
        return topic_name;
    }

    public void setTopic_name(String topic_name) {
        this.topic_name = topic_name;
    }

    public String getUrlkey() {
        return urlkey;
    }

    public void setUrlkey(String urlkey) {
        this.urlkey = urlkey;
    }
}
class Group{
    String group_city  ;
    String group_country  ;
    long group_id  ;
    double group_lat  ;
    double group_lon  ;
    String group_name  ;
    String group_state  ;
    List<Element> group_topics;
    String group_urlname ;

    public String getGroup_city() {
        return group_city;
    }

    public void setGroup_city(String group_city) {
        this.group_city = group_city;
    }

    public String getGroup_country() {
        return group_country;
    }

    public void setGroup_country(String group_country) {
        this.group_country = group_country;
    }

    public long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(long group_id) {
        this.group_id = group_id;
    }

    public double getGroup_lat() {
        return group_lat;
    }

    public void setGroup_lat(double group_lat) {
        this.group_lat = group_lat;
    }

    public double getGroup_lon() {
        return group_lon;
    }

    public void setGroup_lon(double group_lon) {
        this.group_lon = group_lon;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public String getGroup_state() {
        return group_state;
    }

    public void setGroup_state(String group_state) {
        this.group_state = group_state;
    }

    public List<Element> getGroup_topics() {
        return group_topics;
    }

    public void setGroup_topics(List<Element> group_topics) {
        this.group_topics = group_topics;
    }

    public String getGroup_urlname() {
        return group_urlname;
    }

    public void setGroup_urlname(String group_urlname) {
        this.group_urlname = group_urlname;
    }
}
class KafkaLogs{
    Venue venue;
    String visibility;
    long rsvp_id;
    String response;
    long mtime;
    Member member;
    long guests;
    Group group;
    Event event;

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public long getRsvp_id() {
        return rsvp_id;
    }

    public void setRsvp_id(long rsvp_id) {
        this.rsvp_id = rsvp_id;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public Member getMember() {
        return member;
    }

    public void setMember(Member member) {
        this.member = member;
    }

    public long getGuests() {
        return guests;
    }

    public void setGuests(long guests) {
        this.guests = guests;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public Venue getVenue() {
        return venue;
    }

    public void setVenue(Venue venue) {
        this.venue = venue;
    }

}


public class StructuredStreamProcessing {
    public static void main(String[] args) throws Exception{


       // Logger.getGlobal().setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder()
                .appName("structured-streaming")
                .master("local[*]")
                .getOrCreate();
        spark.conf().set("spark.sql.streaming.checkpointLocation", "tmp");


        String country = "us" ;

        Dataset<Row> rows = spark.readStream().
                format("kafka").option("kafka.bootstrap.servers", "10.0.248.57:9092")
                .option("subscribe", "meetup-trending-topics")
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> tempData = spark.read().json("batch/part-00000-20b27ebc-4b74-4307-a5dd-8b5682229f79-c000.txt");
        StructType schema = tempData.schema();


        Dataset<KafkaLogs> values = rows.selectExpr("CAST(value AS STRING)").
                select(functions.from_json(functions.col("value"),schema).as("json")).
                select("json.*").as(Encoders.bean(KafkaLogs.class));

        // .filter( (FilterFunction<KafkaLogs>) value -> value.getGroup().getGroup_country().equals("us"));

        Dataset<Row> valuesWithColumn = values.selectExpr( "mtime"," group.group_country as country", "group.group_city as city", "group.group_topics");

        Dataset<Row> temp = valuesWithColumn.select(col("mtime"), col("country"), col("city"),
                org.apache.spark.sql.functions.explode(col("group_topics")).as("topic"));

        Dataset<Row> flattened = temp.select(col("country"),
                from_unixtime(col("mtime").divide(1000)).as("mtime"),
                temp.col("topic").getField("urlkey").as("url"));


        //filtering step
        Dataset<Row> group_country = flattened.filter("country=='us'").
                withColumn("mtime",col("mtime").cast("timestamp")).
                //withColumn("max", max())
                        withWatermark("mtime", "5 minutes").
                        groupBy(window(col("mtime"), "1 minute", "1 minute"), col("url")).
                        count();

        group_country.printSchema();
        //group_country.

        group_country
                // .withColumn("rank", org.apache.spark.sql.functions.percent_rank().over(Window.partitionBy("window").orderBy(col("count").asc())))
                //.orderBy(col("rank").desc(), col("window").asc())
                .writeStream().
                outputMode("complete").
                format("console").
                start();

        //values.writeStream().outputMode("append").format("text").start("batch");

        //values.printSchema();

        spark.streams().awaitAnyTermination();
        spark.stop();

    }
}
