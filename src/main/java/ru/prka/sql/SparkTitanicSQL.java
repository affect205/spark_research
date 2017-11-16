package ru.prka.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import ru.prka.test.SparkRDD;

import java.net.URL;
import java.util.Objects;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

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
        sqlContext.udf().register("embarked_udf", (String embarked) -> embarked.toLowerCase(), DataTypes.StringType);
        sqlContext.udf().register("family_udf", (Integer clazz, String name, Integer sibSp, Integer parch, String ticket, String embarked) -> Objects.hash(clazz, sibSp > 0 || parch > 0, ticket, embarked), IntegerType);

        URL url = SparkRDD.class.getClassLoader().getResource("test.csv");

        Dataset<Row> testData = sqlContext.read()
                .format("org.apache.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .csv(url.getPath());

        //testData.show(1000,false);

        // группировка по пункту назначения
        // groupByDestinationWithUDF(testData);

        // группировка по семейным отношениям
        groupByFamilyUDF(testData);

        // ранжирование по стоимости билета
        //ticketFareRanker(testData);
    }

    public static void groupByDestinationWithUDF(Dataset<Row> testData) {
        Dataset<Row> data = testData
                .select(col(EMBARKED), col(ID))
                .groupBy(callUDF("embarked_udf", col(EMBARKED)))
                .agg(count(col(ID)).as("pass_count"));
        data.show(1000, false);
    }

    public static void groupByFamilyUDF(Dataset<Row> testData) {
        Dataset<Row> data = testData
                .select(col(CLASS), col(NAME), col(SIBSP), col(PARCH), col(TICKET), col(EMBARKED), col(ID))
                .groupBy(callUDF("family_udf", col(CLASS), col(NAME), col(SIBSP), col(PARCH), col(TICKET), col(EMBARKED)).as("family_id"), col(NAME))
                .agg(count(col(ID)).as("family_count"))
                .orderBy(desc_nulls_last("family_id"));
        data.show(1000, false);
    }

    public static void ticketFareRanker(Dataset<Row> testData) {
        Dataset<Row> data = testData
                .orderBy(desc_nulls_last(FARE));
        data.show(1000, false);
    }
}
