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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

        JavaPairRDD<String, String> playerAndSport = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple2<String, String>(fields[0], fields[3]);
            }
        });

        JavaPairRDD<String, String> sportAndPlayer = playerAndSport.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<String, String>(stringStringTuple2._2, stringStringTuple2._1);
            }
        });

        JavaPairRDD<String, String> sortedSportAndPlayer = sportAndPlayer.sortByKey();

        JavaPairRDD<String, String> sortedPlayerAndSport = sortedSportAndPlayer.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<String, String>(stringStringTuple2._2, stringStringTuple2._1);
            }
        });

        JavaPairRDD<String, String> reduced = sortedPlayerAndSport.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "," + s2;
            }
        });

        JavaPairRDD<String, String> filtered = reduced.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] sports = stringStringTuple2._2.split(",");
                Set<String> uniqueSports = new HashSet<String>(Arrays.asList(sports));
                return uniqueSports.size() > 1;
            }
        });

        JavaPairRDD<String, Integer> result = filtered.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] sports = stringStringTuple2._2.split(",");
                Set<String> uniqueSports = new HashSet<String>(Arrays.asList(sports));
                return new Tuple2<String, Integer>(stringStringTuple2._1, uniqueSports.size());
            }
        });

        System.out.println(result.collect());
    }
}
