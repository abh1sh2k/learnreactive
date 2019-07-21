package com.abhishek.trending;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadSchema {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("read-schema")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("/Users/abhishekkumar/MineProjects/learnreactive/batch/part-00000-20b27ebc-4b74-4307-a5dd-8b5682229f79-c000.txt");
        df.printSchema();
    }
}
