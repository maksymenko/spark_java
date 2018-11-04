package com.sm.kafka_consum;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Handler {
  public static void main(String[] args) throws InterruptedException {
    System.out.println(">>>>>");
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("MyKafkaConsumer");

    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(3));

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "group_id_for_first_test_stream");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);

    Collection<String> topics = Arrays.asList("first_test_topic");

    final JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

    JavaDStream<String> retval = stream.map(
        new Function<ConsumerRecord<String, String>, String>(){

          @Override
          public String call(ConsumerRecord<String, String> record) throws Exception {
            return record.value();
          }
        });
    retval.print();

    JavaPairDStream<String, Integer> pairs = retval.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
    wordCounts.print();


    streamingContext.start();

    streamingContext.awaitTermination();
  }
}
