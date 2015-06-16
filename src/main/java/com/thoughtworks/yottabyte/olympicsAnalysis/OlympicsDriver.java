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
//
//        JavaRDD<Tuple2<String, Integer>> fields = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
//                String[] values = s.split(",");
//                return Arrays.asList(new Tuple2<String, Integer>(values[1], Integer.parseInt(values[7])));
//            }
//        });

        JavaPairRDD<String, Integer> fields = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] values = s.split(",");
                return new Tuple2<String, Integer>(values[1], Integer.parseInt(values[7]));
            }
        });

        JavaPairRDD<String, Integer> reduced = fields.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<Tuple2<Integer, String>> stringIntegerJavaPairRDD = reduced.map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        JavaRDD<Tuple2<Integer, String>> sortedByValue = stringIntegerJavaPairRDD.sortBy(
                new Function<Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        return integerStringTuple2._1;
                    }
                }, false, 1
        );


        System.out.println(sortedByValue.collect());
    }
}
