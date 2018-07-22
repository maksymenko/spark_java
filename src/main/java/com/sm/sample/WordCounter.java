package com.sm.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCounter {
  public static void main(String[] args) {
    System.out.println(">>>> WordCounter");

    SparkConf conf = new SparkConf().setAppName("WordCounter").setMaster("local[3]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/word_count.txt");
    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    Map<String, Long> wordCounts = words.countByValue();

    for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
      System.out.println(entry.getKey() + " : " + entry.getValue());
    }
  }
}
