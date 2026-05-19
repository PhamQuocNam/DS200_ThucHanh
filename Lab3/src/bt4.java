package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class bt4 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Age Group Analysis")
                .setMaster("local");
        
        JavaSparkContext sc = new JavaSparkContext(conf);

 
        JavaRDD<String> movies = sc.textFile("hdfs:///data/movies.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                });

        JavaPairRDD<String, String> movieTitleMap = movies.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) {
                        String[] parts = line.split(",");
                        String movieId = parts[0];
                        String title = parts[1];
                        if (parts.length > 3) {
                            title = line.substring(line.indexOf(",") + 1, line.lastIndexOf(","));
                        }
                        return new Tuple2<String, String>(movieId, title);
                    }
                }
        );


        JavaRDD<String> users = sc.textFile("hdfs:///data/users.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                });

        JavaPairRDD<String, String> userAgeGroupMap = users.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) {
                        String[] parts = line.split(",");
                        String userId = parts[0];
                        int age = Integer.parseInt(parts[2]);
                        
                        String ageGroup = "";
                        if (age < 18) {
                            ageGroup = "<18";
                        } else if (age >= 18 && age <= 35) {
                            ageGroup = "18-35";
                        } else if (age >= 36 && age <= 50) {
                            ageGroup = "36-50";
                        } else {
                            ageGroup = "51+";
                        }
                        
                        return new Tuple2<String, String>(userId, ageGroup);
                    }
                }
        );

 
        JavaRDD<String> ratings1 = sc.textFile("hdfs:///data/ratings_1.txt");
        JavaRDD<String> ratings2 = sc.textFile("hdfs:///data/ratings_2.txt");

        JavaPairRDD<String, Tuple2<String, Double>> userRatingsMap = ratings1.union(ratings2)
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                })
                .mapToPair(
                        new PairFunction<String, String, Tuple2<String, Double>>() {
                            public Tuple2<String, Tuple2<String, Double>> call(String line) {
                                String[] parts = line.split(",");
                                String userId = parts[0];
                                String movieId = parts[1];
                                Double rating = Double.parseDouble(parts[2]);
                                return new Tuple2<String, Tuple2<String, Double>>(
                                        userId, 
                                        new Tuple2<String, Double>(movieId, rating)
                                );
                            }
                        }
                );


        JavaPairRDD<String, Tuple2<String, Tuple2<String, Double>>> joinedData = 
                userAgeGroupMap.join(userRatingsMap);

        JavaPairRDD<String, Tuple2<Double, Integer>> movieAgeGroupKeyMap = joinedData.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Tuple2<String, Double>>>, String, Tuple2<Double, Integer>>() {
                    public Tuple2<String, Tuple2<Double, Integer>> call(
                            Tuple2<String, Tuple2<String, Tuple2<String, Double>>> row) {
                        
                        String ageGroup = row._2._1;
                        String movieId = row._2._2._1;
                        Double rating = row._2._2._2;
                        
                        String key = movieId + "_" + ageGroup;
                        
                        return new Tuple2<String, Tuple2<Double, Integer>>(
                                key, 
                                new Tuple2<Double, Integer>(rating, 1)
                        );
                    }
                }
        );

        JavaPairRDD<String, Tuple2<Double, Integer>> ratingStats = movieAgeGroupKeyMap.reduceByKey(
                new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<Double, Integer>(a._1 + b._1, a._2 + b._2);
                    }
                }
        );

        JavaPairRDD<String, Double> averageRatingsMap = ratingStats.mapValues(
                new Function<Tuple2<Double, Integer>, Double>() {
                    public Double call(Tuple2<Double, Integer> value) {
                        return value._1 / value._2;
                    }
                }
        );

        JavaPairRDD<String, Tuple2<String, Double>> movieAvgWithAgeGroupMap = averageRatingsMap.mapToPair(
                new PairFunction<Tuple2<String, Double>, String, Tuple2<String, Double>>() {
                    public Tuple2<String, Tuple2<String, Double>> call(Tuple2<String, Double> row) {
                        String compositeKey = row._1;
                        String[] parts = compositeKey.split("_");
                        String movieId = parts[0];
                        String ageGroup = parts[1];
                        Double avgRating = row._2;
                        
                        return new Tuple2<String, Tuple2<String, Double>>(
                                movieId, 
                                new Tuple2<String, Double>(ageGroup, avgRating)
                        );
                    }
                }
        );

        JavaPairRDD<String, Tuple2<Tuple2<String, Double>, String>> finalResults = 
                movieAvgWithAgeGroupMap.join(movieTitleMap);

        String outputFilePath = "ketqua_nhomtuoi.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            writer.println("========== ĐÁNH GIÁ PHIM THEO NHÓM TUỔI ==========");
            writer.println(String.format("%-10s | %-35s | %-12s | %s", "MovieID", "TITLE", "AGE GROUP", "AVG RATING"));
            writer.println("----------------------------------------------------------------------------------");

            for (Tuple2<String, Tuple2<Tuple2<String, Double>, String>> record : finalResults.collect()) {
                String movieId = record._1;
                String ageGroup = record._2._1._1;
                Double avgRating = record._2._1._2;
                String title = record._2._2;

                if (title.length() > 32) {
                    title = title.substring(0, 30) + "...";
                }

                writer.println(String.format("%-10s | %-35s | %-12s | %.2f", 
                        movieId, title, ageGroup, avgRating));
            }

            System.out.println("Đã phân tích xong! Kết quả được lưu tại: " + outputFilePath);

        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
            e.printStackTrace();
        }

        sc.stop();
    }
}