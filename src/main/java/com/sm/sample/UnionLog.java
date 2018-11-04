package com.sm.sample;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class UnionLog {
  private static final String LOG_FILE_1 = "src/main/resources/nasa_19950701.tsv";
  private static final String LOG_FILE_2 = "src/main/resources/nasa_19950801.tsv";
  private static final String OUT_PATH = "res/logs_union";

  public static void main(String[] args) throws IOException {
    System.out.println(">>> Log Union");

    SparkConf conf = new SparkConf().setAppName("Log Union").setMaster("local[*]");
    JavaSparkContext spakCtx = new JavaSparkContext(conf);

    JavaRDD<String> firstLog = spakCtx.textFile(LOG_FILE_1);

    FileUtils.deleteDirectory(new File(OUT_PATH));

    firstLog
        .union(spakCtx.textFile(LOG_FILE_2))
        .filter(line -> !(line.startsWith("host") && line.contains("bytes")))
        .sample(false, 0.01).saveAsTextFile(OUT_PATH);


  }
}
