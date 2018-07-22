package com.sm.sample;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInCountry {
  public static void main(String[] args) {
    System.out.println(">>>> Airports in country");

    SparkConf conf = new SparkConf().setAppName("Airport in  country").setMaster("local[*]");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> lines = context.textFile("src/main/resources/airports.text");

    lines = lines.filter(line -> line.split(",")[3].equals("\"United States\""));

    JavaRDD<String> airportCity = lines.map(line -> {
      String[] arr = line.split(",");
      return StringUtils.join(new String[]{arr[1], arr[2]}, ",");
    });

    airportCity.saveAsTextFile("res/airports_city.out");

  }
}
