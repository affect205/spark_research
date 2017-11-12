package ru.prka.ml;

import org.apache.spark.sql.SparkSession;
import ru.prka.test.SparkRDD;

import java.net.URL;

/**
 * Created by Alex on 05.11.2017.
 */
public class SparkTitanicModel {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("json processor")
                .config("spark.master", "local")
                .getOrCreate();

        URL url = SparkRDD.class.getClassLoader().getResource("test.csv");


    }

}
