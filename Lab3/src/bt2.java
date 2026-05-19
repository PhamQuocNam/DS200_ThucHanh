package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class bt2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Genre Analysis")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> movies = sc.textFile("hdfs:///data/movies.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                });

        JavaPairRDD<String, String> movieGenresMap = movies.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) {
                        String[] parts = line.split(",");
                        String movieId = parts[0];
                        // Giả sử thể loại nằm ở cột thứ 3 (index 2)
                        String genres = (parts.length >= 3) ? parts[2] : "Unknown"; 
                        return new Tuple2<String, String>(movieId, genres);
                    }
                }
        );

        JavaRDD<String> ratings1 = sc.textFile("hdfs:///data/ratings_1.txt");
        JavaRDD<String> ratings2 = sc.textFile("hdfs:///data/ratings_2.txt");
        
        JavaPairRDD<String, Tuple2<Double, Integer>> movieRatings = ratings1.union(ratings2)
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                })
                .mapToPair(
                        new PairFunction<String, String, Tuple2<Double, Integer>>() {
                            public Tuple2<String, Tuple2<Double, Integer>> call(String line) {
                                String[] parts = line.split(",");
                                String movieId = parts[1];
                                Double rating = Double.parseDouble(parts[2]);
                                return new Tuple2<String, Tuple2<Double, Integer>>(
                                        movieId,
                                        new Tuple2<Double, Integer>(rating, 1)
                                );
                            }
                        }
                );

        
        JavaPairRDD<String, Tuple2<String, Tuple2<Double, Integer>>> joinedData = 
                movieGenresMap.join(movieRatings);

        JavaPairRDD<String, Tuple2<Double, Integer>> genreRatings = joinedData.flatMapToPair(
                new PairFlatMapFunction<
                        Tuple2<String, Tuple2<String, Tuple2<Double, Integer>>>, 
                        String, 
                        Tuple2<Double, Integer>>() {
                    
                    public Iterable<Tuple2<String, Tuple2<Double, Integer>>> call(
                            Tuple2<String, Tuple2<String, Tuple2<Double, Integer>>> row) {
                        
                        String genresString = row._2._1; // VD: "Action|Comedy"
                        Tuple2<Double, Integer> ratingData = row._2._2; // (rating, 1)
                        
                        String[] genresArray = genresString.split("\\|");
                        
                        List<Tuple2<String, Tuple2<Double, Integer>>> resultList = new ArrayList<>();
                        for (String genre : genresArray) {
                            resultList.add(new Tuple2<String, Tuple2<Double, Integer>>(genre.trim(), ratingData));
                        }
                        return resultList;
                    }
                }
        );

       
        JavaPairRDD<String, Tuple2<Double, Integer>> genreStats = genreRatings.reduceByKey(
                new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<Double, Integer>(
                                a._1 + b._1, // Tổng điểm
                                a._2 + b._2  // Tổng lượt
                        );
                    }
                }
        );

        JavaPairRDD<String, Double> genreAverages = genreStats.mapValues(
                new Function<Tuple2<Double, Integer>, Double>() {
                    public Double call(Tuple2<Double, Integer> value) {
                        return value._1 / value._2;
                    }
                }
        );

        JavaPairRDD<Double, String> sortedGenres = genreAverages
                .mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
                    public Tuple2<Double, String> call(Tuple2<String, Double> item) {
                        return new Tuple2<Double, String>(item._2, item._1);
                    }
                })
                .sortByKey(false);

        String outputFilePath = "ketqua_theloai.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            writer.println("========== ĐIỂM TRUNG BÌNH THEO THỂ LOẠI ==========");
            writer.println(String.format("%-20s | %s", "THỂ LOẠI", "ĐIỂM TRUNG BÌNH"));
            writer.println("---------------------------------------------------");

            for (Tuple2<Double, String> record : sortedGenres.collect()) {
                Double avgRating = record._1;
                String genreName = record._2;
                
                writer.println(String.format("%-20s | %.2f", genreName, avgRating));
            }

            System.out.println("Đã phân tích xong! Kết quả được lưu tại: " + outputFilePath);

        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
            e.printStackTrace();
        }

        sc.stop();
    }
}