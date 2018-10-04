package ru.prka.test;

import org.apache.spark.sql.SparkSession;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Alex on 04.10.2018.
 * Задача: подсчитать количество повторяющихся строк с помощью spark:
 * Например, есть csv файл со следующими строками:
 * A // 1
 * A
 * A
 * B // 2
 * A // 3
 * A
 * C // 4
 * C
 * C
 * C
 * G // 5
 * B // 6
 * A // 7
 * A
 * Как видно, для данного файла количество повторяющихся строк равно 7
 */
public class SeqCounter {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Sequence counter")
                .config("spark.master", "local")
                .getOrCreate();
        int seqCount = seqCounter(spark);
        System.out.printf("Sequences count is: %s\n", seqCount);
    }

    public static int seqCounter(SparkSession spark) {
        final AtomicReference<String> last = new AtomicReference<>(null);
        return spark.read()
                .csv("data.csv")
                .javaRDD()
                .map(row -> {
                    System.out.println(row.getString(0));
                    return row.getString(0);
                })
                .aggregate(0, (cnt, vl) -> {
                    if (!Objects.equals(last.get(), vl)) {
                        last.set(vl);
                        return cnt++;
                    }
                    last.set(vl);
                    return cnt;
                }, Integer::sum);
    }
}
