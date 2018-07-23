package com.sm.sample;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class AirportByLatitude {
  private static final String DELIMETER = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)";
  private static final String IN_FILE_PATH = "src/main/resources/airports.text";
  private static final String OUT_PATH = "res/airport_lat";

  public static void main(String[] args) throws IOException {
    System.out.println(">>>> Airport by latitude");

    SparkConf conf = new SparkConf().setAppName("Airport by latitude").setMaster("local[*]");
    JavaSparkContext sparkCtx = new JavaSparkContext(conf);

    JavaRDD<String> airports = sparkCtx.textFile(IN_FILE_PATH)
        .filter(line -> {
          String latStr = line.split(DELIMETER)[6];
          return Double.parseDouble(latStr) > 40;
        }).map(line -> {
          String[] arr = line.split(DELIMETER);
          return StringUtils.join(new String[]{arr[1], arr[2], arr[6]}, " : ");
        });

    FileUtils.deleteDirectory(new File(OUT_PATH));

    airports.saveAsTextFile(OUT_PATH);
  }
}
