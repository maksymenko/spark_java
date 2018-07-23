package com.sm.sample;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class AirportsInCountry {
  private static final String DELIMETER = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)";
  private static final String IN_FILE_PATH = "src/main/resources/airports.text";
  private static final String OUT_PATH = "res/airports_city";

  public static void main(String[] args) throws IOException {
    System.out.println(">>>> Airports in country");

    SparkConf conf = new SparkConf().setAppName("Airport in  country").setMaster("local[1]");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> lines = context.textFile(IN_FILE_PATH);

    lines = lines.filter(line -> line.split(DELIMETER)[3].equals("\"United States\""));

    JavaRDD<String> airportCity = lines.map(line -> {
      String[] arr = line.split(DELIMETER);
      return StringUtils.join(new String[]{arr[1], arr[2]}, ",");
    });

    FileUtils.deleteDirectory(new File(OUT_PATH));
    airportCity.saveAsTextFile(OUT_PATH);

  }
}
