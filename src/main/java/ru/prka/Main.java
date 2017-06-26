package ru.prka;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * Created by abalyshev on 14.06.17.
 */
public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("json processor")
                .config("spark.master", "local")
                .getOrCreate();

        URL url = Main.class.getClassLoader().getResource("test.csv");

        JavaRDD<Row> rdd = spark.read().csv(url.getPath()).javaRDD();
        //rdd.map(row -> System.out.println(row.toString()));

        System.out.println("Mans only:");
        rdd
                .filter(row -> "male".equals(row.getString(3)))
                .map(row -> row.getString(2))
                .foreach(name -> System.out.println(name));

        System.out.println("============================================================");
        System.out.println("Womans only:");
        rdd
                .filter(row -> "female".equals(row.getString(3)))
                .map(row -> row.getString(2))
                .foreach(name -> System.out.println(name));


//        JavaRDD<Row> rdd = spark.read().json(url.getPath()).javaRDD();
//
//        Object[] names = rdd
//                .map(line -> line.getString(line.fieldIndex("name")))
//                .sortBy(s1 -> s1, true, 1)
//                .collect()
//                .toArray();
//
//        System.out.println(Arrays.toString(names));
    }

    public static void completableTest() {
        System.out.printf("[TID:%s] completableTest...\n", Thread.currentThread().getId());
        CompletableFuture<Void> future = CompletableFuture
                .supplyAsync(Main::getString)
                .thenAccept(Main::acceptResult);
    }

    public static String getString() {
        System.out.printf("[TID:%s] getString...\n", Thread.currentThread().getId());
        return "result";
    }

    public static void acceptResult(String result) {
        System.out.printf("[TID:%s] acceptResult...\n", Thread.currentThread().getId());
        System.out.printf("result is: %s\n", result);
    }
}
