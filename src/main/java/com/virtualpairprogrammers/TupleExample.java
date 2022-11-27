package com.virtualpairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TupleExample {
    public static void main(String[] args) {
        List<Double> doubleCode = new ArrayList<Double>();
        doubleCode.add(16.00);
        doubleCode.add(9.00);
        doubleCode.add(64.00);
        doubleCode.add(121.00);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Start spark").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<Double> originalRDD = context.parallelize(doubleCode);

        JavaRDD<Tuple2<Double, Double>> sqrtRDD = originalRDD.map(value -> new Tuple2(value, Math.sqrt(value)));

        sqrtRDD.foreach((tuple2) -> System.out.println("Square root of " + tuple2._1() + " is : " + tuple2._2()));
    }
}
