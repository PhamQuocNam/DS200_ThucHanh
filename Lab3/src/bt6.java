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
import java.util.Calendar;

public class bt6 {

    public static void main(String[] args) {

     
        SparkConf conf = new SparkConf()
                .setAppName("Yearly Rating Analysis")
                .setMaster("local");
        
        JavaSparkContext sc = new JavaSparkContext(conf);

 
        JavaRDD<String> ratings1 = sc.textFile("hdfs:///data/ratings_1.txt");
        JavaRDD<String> ratings2 = sc.textFile("hdfs:///data/ratings_2.txt");

        JavaRDD<String> allRatings = ratings1.union(ratings2)
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String line) {
                        return line != null && line.contains(",");
                    }
                });

       
        JavaPairRDD<String, Tuple2<Double, Integer>> yearRatingPairs = allRatings.mapToPair(
                new PairFunction<String, String, Tuple2<Double, Integer>>() {
                    public Tuple2<String, Tuple2<Double, Integer>> call(String line) {
                        String[] parts = line.split(",");
                        
                        // Rating nằm ở index 2
                        Double rating = Double.parseDouble(parts[2]);
                        
                        // Timestamp nằm ở index 3
                        long timestamp = Long.parseLong(parts[3]);
                        
                        // Chuyển đổi Timestamp (giây) sang Year
                        // Nhân 1000L để đổi giây thành mili-giây cho thư viện Java
                        Calendar cal = Calendar.getInstance();
                        cal.setTimeInMillis(timestamp * 1000L);
                        String year = String.valueOf(cal.get(Calendar.YEAR));
                        
                        return new Tuple2<String, Tuple2<Double, Integer>>(
                                year, 
                                new Tuple2<Double, Integer>(rating, 1)
                        );
                    }
                }
        );


        JavaPairRDD<String, Tuple2<Double, Integer>> yearlyStats = yearRatingPairs.reduceByKey(
                new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<Double, Integer>(
                                a._1 + b._1, // Tổng điểm
                                a._2 + b._2  // Tổng số lượt đánh giá
                        );
                    }
                }
        );

        // Tính Average = Tổng / Số lượng
        // Kết quả: Year -> (AvgRating, TotalCount)
        JavaPairRDD<String, Tuple2<Double, Integer>> yearlyAverages = yearlyStats.mapValues(
                new Function<Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> value) {
                        Double avgRating = value._1 / value._2;
                        return new Tuple2<Double, Integer>(avgRating, value._2);
                    }
                }
        );


        // sortByKey(true) để sắp xếp tăng dần từ năm cũ đến năm mới
        JavaPairRDD<String, Tuple2<Double, Integer>> sortedYearlyData = yearlyAverages.sortByKey(true);


        String outputFilePath = "ketqua_theonam.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            writer.println("========== ĐÁNH GIÁ TRUNG BÌNH THEO TỪNG NĂM ==========");
            writer.println(String.format("%-10s | %-15s | %s", "NĂM", "ĐIỂM TRUNG BÌNH", "TỔNG SỐ LƯỢT ĐÁNH GIÁ"));
            writer.println("----------------------------------------------------------------");

            for (Tuple2<String, Tuple2<Double, Integer>> record : sortedYearlyData.collect()) {
                String year = record._1;
                Double avgRating = record._2._1;
                Integer totalCount = record._2._2;

                writer.println(String.format("%-10s | %-15.2f | %d", year, avgRating, totalCount));
            }

            System.out.println("Đã phân tích xong! Kết quả được lưu tại: " + outputFilePath);

        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
            e.printStackTrace();
        }

        sc.stop();
    }
}