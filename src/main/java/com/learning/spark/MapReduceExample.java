package com.learning.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class MapReduceExample {

    public static void main(String[] args) {

        List<Double> doubleCode = new ArrayList<Double>();
        doubleCode.add(16.00);
        doubleCode.add(9.00);
        doubleCode.add(64.00);
        doubleCode.add(121.00);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Start spark").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<Double> myRdd = context.parallelize(doubleCode);
//        System.out.println(myRdd.reduce((x, y) -> x+y));

        JavaDoubleRDD sqrtRdd = myRdd.mapToDouble(e -> Math.sqrt(e));
        sqrtRdd.foreach((e) -> System.out.println(e));

        JavaRDD<Double> mapRdd = sqrtRdd.map((x) -> x*2.0);
        System.out.println(mapRdd.reduce((val1, val2) -> val1+val2));

        context.close();
    }
}
