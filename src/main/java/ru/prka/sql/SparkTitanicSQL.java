package ru.prka.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import ru.prka.test.SparkRDD;

import java.net.URL;

import static org.apache.spark.sql.functions.*;

/**
 * Created by abalyshev on 15.11.17.
 */
public class SparkTitanicSQL {
    public static final String ID = "PassengerId";
    public static final String CLASS = "Pclass";
    public static final String NAME = "Name";
    public static final String SEX = "Sex";
    public static final String AGE = "Age";
    public static final String SIBSP = "SibSp"; // наличие на борту родственников/супругов. 0 - нет
    public static final String PARCH = "Parch"; // наличие на борту детей/родителей. 0 - нет
    public static final String TICKET = "Ticket"; // номер билета
    public static final String FARE = "Fare"; // стоимость билета
    public static final String CABIN = "Cabin";
    public static final String EMBARKED = "Embarked"; // пункт назначения

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("json processor")
                .config("spark.master", "local")
                .getOrCreate();

        SQLContext sqlContext = spark.sqlContext();

        URL url = SparkRDD.class.getClassLoader().getResource("test.csv");

        Dataset<Row> testData = sqlContext.read()
                .format("org.apache.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .csv(url.getPath());

        //testData.show(1000,false);

        // группировка по пункту назначения
        groupByDestination(testData);

        // группировка по семейным отношениям

        // ранжирование по стоимости билета
        richerRanker(testData);
    }

    public static void groupByDestination(Dataset<Row> testData) {
        Dataset<Row> data = testData
                .select(col(EMBARKED), col(ID))
                .groupBy(col(EMBARKED))
                .agg(count(col(ID)).as("pass_count"));
        data.show(1000, false);
    }

    public static void groupByFamily(Dataset<Row> testData) {

    }

    public static void richerRanker(Dataset<Row> testData) {
        Dataset<Row> data = testData
                .orderBy(desc_nulls_last(FARE));
        data.show(1000, false);
    }
}
