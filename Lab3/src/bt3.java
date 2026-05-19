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

public class bt3 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Gender Analysis")
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

        JavaPairRDD<String, String> userGenderMap = users.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) {
                        String[] parts = line.split(",");
                        String userId = parts[0];
                        String gender = parts[1];
                        return new Tuple2<String, String>(userId, gender);
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
                userGenderMap.join(userRatingsMap);

        JavaPairRDD<String, Tuple2<Double, Integer>> movieGenderKeyMap = joinedData.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Tuple2<String, Double>>>, String, Tuple2<Double, Integer>>() {
                    public Tuple2<String, Tuple2<Double, Integer>> call(
                            Tuple2<String, Tuple2<String, Tuple2<String, Double>>> row) {
                        
                        String gender = row._2._1;
                        String movieId = row._2._2._1;
                        Double rating = row._2._2._2;
                        
                        String key = movieId + "_" + gender;
                        
                        return new Tuple2<String, Tuple2<Double, Integer>>(
                                key, 
                                new Tuple2<Double, Integer>(rating, 1)
                        );
                    }
                }
        );

        JavaPairRDD<String, Tuple2<Double, Integer>> ratingStats = movieGenderKeyMap.reduceByKey(
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

     
        JavaPairRDD<String, Tuple2<String, Double>> movieAvgWithGenderMap = averageRatingsMap.mapToPair(
                new PairFunction<Tuple2<String, Double>, String, Tuple2<String, Double>>() {
                    public Tuple2<String, Tuple2<String, Double>> call(Tuple2<String, Double> row) {
                        String compositeKey = row._1;
                        String[] parts = compositeKey.split("_");
                        String movieId = parts[0];
                        String gender = parts[1];
                        Double avgRating = row._2;
                        
                        return new Tuple2<String, Tuple2<String, Double>>(
                                movieId, 
                                new Tuple2<String, Double>(gender, avgRating)
                        );
                    }
                }
        );

  
        JavaPairRDD<String, Tuple2<Tuple2<String, Double>, String>> finalResults = 
                movieAvgWithGenderMap.join(movieTitleMap);

        String outputFilePath = "ketqua_gioitinh.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            writer.println("========== ĐÁNH GIÁ PHIM THEO GIỚI TÍNH ==========");
            writer.println(String.format("%-10s | %-40s | %-10s | %s", "MovieID", "TITLE", "GENDER", "AVG RATING"));
            writer.println("----------------------------------------------------------------------------------");

            for (Tuple2<String, Tuple2<Tuple2<String, Double>, String>> record : finalResults.collect()) {
                String movieId = record._1;
                String gender = record._2._1._1;
                Double avgRating = record._2._1._2;
                String title = record._2._2;

                if (title.length() > 37) {
                    title = title.substring(0, 35) + "...";
                }

                writer.println(String.format("%-10s | %-40s | %-10s | %.2f", 
                        movieId, title, (gender.equals("M") ? "Nam" : "Nữ"), avgRating));
            }

            System.out.println("Đã phân tích xong! Kết quả được lưu tại: " + outputFilePath);

        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
            e.printStackTrace();
        }


        sc.stop();
    }
}