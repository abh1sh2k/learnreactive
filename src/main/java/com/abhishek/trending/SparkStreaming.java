package com.abhishek.trending;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jackson.map.ObjectMapper;
import org.json4s.jackson.Json;
import scala.Array;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class MeetUpLog{
    long mtime ;
    String city;
    String country;
    String urlkey ;
    MeetUpLog(long mtime ,String city, String country ,String urlkey){
        this.mtime = mtime ;
        this.city = city;
        this.country = country;
        this.urlkey = urlkey ;
    }

    @Override
    public String toString() {
        return "mtime = " +mtime + ", city = "  +city +
                ", country = " + country +
                ", urlkey = " +urlkey ;
    }
}
/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;

    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}

public class SparkStreaming {

    static List<MeetUpLog> getMeetupLog(String jsonString) throws IOException{
        List<MeetUpLog> meetuplogs = new ArrayList<>();
        try {


            KafkaLogs logs = new ObjectMapper().readValue(jsonString, KafkaLogs.class);

            Iterator<Element> iterator = logs.getGroup().group_topics.iterator();
            while (iterator.hasNext()) {
                meetuplogs.add((new MeetUpLog(logs.mtime, logs.group.group_city, logs.group.group_country, iterator.next().urlkey)));
            }
        } catch(IOException ex) {
            System.out.println("error for ser >>>>> " + jsonString);
            throw ex;

        }
        return meetuplogs;

    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Logger.getGlobal().setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("streaming").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.0.248.57:9092");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "spark_streaming");

        Collection<String> topics = Arrays.asList("meetup-trending-topics");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> records = stream.map(record -> record.value());
        JavaDStream<MeetUpLog> meetUps = records.flatMap(new FlatMapFunction<String, MeetUpLog>() {
            @Override public Iterator<MeetUpLog> call(String s) throws Exception {
                List<MeetUpLog> logs = getMeetupLog(s);
                  return logs.iterator();
               }
            }
        ).filter(meetUp -> meetUp.country.equals("us"));

        JavaPairDStream<String, Integer> windowCount = meetUps.mapToPair(m -> new Tuple2<String, Integer>(m.urlkey, 1)).
                reduceByKey((s1, s2) -> (s1 + s2))
                .window(new Duration(10000), new Duration(5000));
        //windowCount.for


        windowCount.foreachRDD((rdd, time) -> {
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
            JavaRDD<WordCount> rowRDD = (JavaRDD<WordCount>) rdd.map(word -> {
                WordCount record = new WordCount();
                record.setWord(word._1);
                record.setCount(word._2);
                return record;
            });

            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, WordCount.class);

            wordsDataFrame.printSchema();
            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count from words order by count desc limit 10");
            System.out.println("========= " + time + "=========");
            wordCountsDataFrame.show();
        });
        ssc.start();
        ssc.awaitTermination();
    }


}
