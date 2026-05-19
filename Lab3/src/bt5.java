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

public class bt5 {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
                .setAppName("Occupation Analysis")
                .setMaster("local")
                .set("spark.driver.host", "127.0.0.1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> occupations = sc.textFile("hdfs:///data/occupation.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                });

        JavaPairRDD<String, String> occupationMap = occupations.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) {
                        String[] parts = line.split(",");
                        return new Tuple2<String, String>(parts[0], parts[1]);
                    }
                }
        );


        JavaRDD<String> users = sc.textFile("hdfs:///data/users.txt")
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                });

        JavaPairRDD<String, String> userOccMap = users.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String line) {
                        String[] parts = line.split(",");
                        String userId = parts[0];
                        // OccupationID nằm ở cột thứ 4 (index 3)
                        String occId = parts[3]; 
                        return new Tuple2<String, String>(userId, occId);
                    }
                }
        );


        JavaRDD<String> ratings1 = sc.textFile("hdfs:///data/ratings_1.txt");
        JavaRDD<String> ratings2 = sc.textFile("hdfs:///data/ratings_2.txt");

        JavaPairRDD<String, Double> userRatingsMap = ratings1.union(ratings2)
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                })
                .mapToPair(
                        new PairFunction<String, String, Double>() {
                            public Tuple2<String, Double> call(String line) {
                                String[] parts = line.split(",");
                                String userId = parts[0];
                                Double rating = Double.parseDouble(parts[2]);
                                return new Tuple2<String, Double>(userId, rating);
                            }
                        }
                );


        JavaPairRDD<String, Tuple2<String, Double>> joinedData = userOccMap.join(userRatingsMap);

        JavaPairRDD<String, Tuple2<Double, Integer>> occRatingPairs = joinedData.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Double>>, String, Tuple2<Double, Integer>>() {
                    public Tuple2<String, Tuple2<Double, Integer>> call(
                            Tuple2<String, Tuple2<String, Double>> row) {
                        
                        String occId = row._2._1;
                        Double rating = row._2._2;
                        
                        return new Tuple2<String, Tuple2<Double, Integer>>(
                                occId, 
                                new Tuple2<Double, Integer>(rating, 1)
                        );
                    }
                }
        );

   
        // Tính tổng điểm và tổng lượt vote
        JavaPairRDD<String, Tuple2<Double, Integer>> occStats = occRatingPairs.reduceByKey(
                new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<Double, Integer>(a._1 + b._1, a._2 + b._2);
                    }
                }
        );

        // Tính Average = Tổng / Số lượng
        // Kết quả: OccupationID -> (AvgRating, TotalCount)
        JavaPairRDD<String, Tuple2<Double, Integer>> occAverages = occStats.mapValues(
                new Function<Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> value) {
                        Double avgRating = value._1 / value._2;
                        return new Tuple2<Double, Integer>(avgRating, value._2);
                    }
                }
        );

  
        JavaPairRDD<String, Tuple2<Tuple2<Double, Integer>, String>> finalData = occAverages.join(occupationMap);

        // Sắp xếp theo điểm trung bình giảm dần
        JavaPairRDD<Double, Tuple2<String, Integer>> sortedResults = finalData.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Tuple2<Double, Integer>, String>>, Double, Tuple2<String, Integer>>() {
                    public Tuple2<Double, Tuple2<String, Integer>> call(
                            Tuple2<String, Tuple2<Tuple2<Double, Integer>, String>> row) {
                        
                        Double avgRating = row._2._1._1;
                        Integer totalCount = row._2._1._2;
                        String occName = row._2._2;
                        
                        return new Tuple2<Double, Tuple2<String, Integer>>(
                                avgRating, 
                                new Tuple2<String, Integer>(occName, totalCount)
                        );
                    }
                }
        ).sortByKey(false);


        String outputFilePath = "ketqua_nghenghiep.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            writer.println("========== ĐÁNH GIÁ TRUNG BÌNH THEO NGHỀ NGHIỆP ==========");
            writer.println(String.format("%-25s | %-15s | %s", "NGHỀ NGHIỆP", "ĐIỂM TRUNG BÌNH", "TỔNG SỐ LƯỢT ĐÁNH GIÁ"));
            writer.println("----------------------------------------------------------------------");

            // Sắp xếp giảm dần nên RDD đang lưu key là Double (AvgRating)
            for (Tuple2<Double, Tuple2<String, Integer>> record : sortedResults.collect()) {
                Double avgRating = record._1;
                String occName = record._2._1;
                Integer totalCount = record._2._2;

                writer.println(String.format("%-25s | %-15.2f | %d", occName, avgRating, totalCount));
            }

            System.out.println("Đã phân tích xong! Kết quả được lưu tại: " + outputFilePath);

        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
            e.printStackTrace();
        }

 
        sc.stop();
    }
}