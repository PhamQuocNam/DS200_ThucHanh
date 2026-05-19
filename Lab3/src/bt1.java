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

public class bt1 {

    public static void main(String[] args) {

   
        SparkConf conf = new SparkConf()
                .setAppName("Movie Analysis")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> movies =
                sc.textFile("hdfs:///data/movies.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null
                                && line.contains(",");
                    }
                });

        JavaPairRDD<String, String> movieMap =
                movies.mapToPair(
                        new PairFunction<String, String, String>() {
                            public Tuple2<String, String> call(String line) {
                                String[] parts = line.split(",");
                                return new Tuple2<String, String>(
                                        parts[0], // MovieID
                                        parts[1]  // Title
                                );
                            }
                        }
                );


        JavaRDD<String> ratings1 =
                sc.textFile("hdfs:///data/ratings_1.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null
                                && line.contains(",");
                    }
                });

        JavaRDD<String> ratings2 =
                sc.textFile("hdfs:///data/ratings_2.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null
                                && line.contains(",");
                    }
                });

        JavaRDD<String> ratings =
                ratings1.union(ratings2);

        JavaPairRDD<String, Tuple2<Double, Integer>> movieRatings =
                ratings.mapToPair(
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

    

        JavaPairRDD<String, Tuple2<Double, Integer>> ratingStats =
                movieRatings.reduceByKey(
                        new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                                return new Tuple2<Double, Integer>(
                                        a._1 + b._1, // tổng điểm
                                        a._2 + b._2  // tổng lượt đánh giá
                                );
                            }
                        }
                );

  

        JavaPairRDD<String, Tuple2<Double, Integer>> averageRatings =
                ratingStats.mapValues(
                        new Function<Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> value) {
                                Double avg = value._1 / value._2;
                                return new Tuple2<Double, Integer>(
                                        avg,
                                        value._2
                                );
                            }
                        }
                );

  

        JavaPairRDD<String, Tuple2<Double, Integer>> filteredMovies =
                averageRatings.filter(
                        new Function<Tuple2<String, Tuple2<Double, Integer>>, Boolean>() {
                            public Boolean call(Tuple2<String, Tuple2<Double, Integer>> x) {
                                return x._2._2 >= 5;
                            }
                        }
                );

        JavaPairRDD<String, Tuple2<Tuple2<Double, Integer>, String>> movieDetails =
                filteredMovies.join(movieMap);


        JavaPairRDD<Double, String> sortedMovies =
                movieDetails.mapToPair(
                        new PairFunction<Tuple2<String, Tuple2<Tuple2<Double, Integer>, String>>, Double, String>() {
                            public Tuple2<Double, String> call(Tuple2<String, Tuple2<Tuple2<Double, Integer>, String>> x) {
                                Double avg = x._2._1._1;
                                String title = x._2._2;
                                return new Tuple2<Double, String>(
                                        avg,
                                        title
                                );
                            }
                        }
                ).sortByKey(false);


        String outputFilePath = "ketqua_movies.txt"; 

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            
            writer.println("========== DANH SÁCH PHIM ==========");

            for (Tuple2<String, Tuple2<Tuple2<Double, Integer>, String>> movie : movieDetails.collect()) {
                String movieId = movie._1;
                Double avgRating = movie._2._1._1;
                Integer totalRatings = movie._2._1._2;
                String title = movie._2._2;

                writer.println(
                        "MovieID: " + movieId
                        + " | Title: " + title
                        + " | Average Rating: " + avgRating
                        + " | Total Ratings: " + totalRatings
                );
            }

    
            Tuple2<Double, String> topMovie = sortedMovies.first();

            writer.println("\n========== TOP MOVIE ==========");
            writer.println("Phim có điểm cao nhất : " + topMovie._2);
            writer.println("Điểm trung bình       : " + topMovie._1);
            
            System.out.println("Đã ghi kết quả thành công vào file: " + outputFilePath);

        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
            e.printStackTrace();
        }

        sc.stop();
    }
}