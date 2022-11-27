package com.virtualpairprogrammers.pairRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

public class LogCounterPairRDDExample implements Serializable {

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                .setMaster("local[*]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // provide path to input text file
        String path = "localhost.2008-06-29.log";

        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(path);
        lines.foreach(line -> System.out.println(line + " ---- "));
        JavaPairRDD<String, Long> pairRDD = lines.mapToPair(value -> {
            String arr[] = value.split(":");
            String key = arr[0];
            String val = arr[1];
            return new Tuple2<>(arr[0], 1L);
        });

        JavaPairRDD<String, Long> sumRDD = pairRDD.reduceByKey((val1, val2) -> val1 + val2);

        sumRDD.foreach(element -> System.out.println(element._1 + "  " + element._2));

    }
}
