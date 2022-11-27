package com.learning.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapExample {


    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        List<String> inputData = new ArrayList<>();
        inputData.add("Name: Shantanu Sahay");
        inputData.add("Age: 40");
        inputData.add("Sex: Male");
        inputData.add("Email: shantanu.sahay@gmail..com shantanu.sahay@google.com");
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("FlatMap App")
                .setMaster("local[*]");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.parallelize(inputData);

        // provide path to input text file

        // read text file to RDD
        lines.foreach( element -> System.out.println(element));

        JavaRDD<String> words = lines.flatMap(val -> Arrays.asList(val.split(" ")).iterator());
        words.foreach(element -> System.out.println(element));

        JavaRDD<List<String>> wordMap = lines.map(val -> Arrays.asList(val.split(" ")));
        wordMap.foreach(e -> System.out.println(e));
        sc.close();

    }

}
