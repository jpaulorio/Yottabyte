package com.thoughtworks.yottabyte.olympicsAnalysis;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class OlympicsDriver {
    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String path = args[2];
        String destination = args[3];

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Logger log = Logger.getLogger(OlympicsDriver.class.getName());

        JavaRDD<String> lines = sc.textFile(path).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.isEmpty() && !s.contains("Total");
            }
        });

        JavaRDD<String> usData = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("United States");
            }
        });

        JavaPairRDD<Integer, Integer> usYearsAndMedals = usData.mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple2<Integer, Integer>(Integer.parseInt(fields[2]), Integer.parseInt(fields[7]));
            }
        });

        JavaPairRDD<Integer, Integer> reducedByYear = usYearsAndMedals.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<Integer, Integer> usMedalsAndYear = reducedByYear.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return new Tuple2<Integer, Integer>(integerIntegerTuple2._2, integerIntegerTuple2._1);
            }
        });

        JavaPairRDD<Integer, Integer> sortedByMedals = usMedalsAndYear.sortByKey(false);

        System.out.println(sortedByMedals.collect());
    }
}
