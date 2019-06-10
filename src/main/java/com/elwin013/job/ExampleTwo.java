package com.elwin013.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ExampleTwo {

    private final SQLContext sqlContext;

    public ExampleTwo() {

        sqlContext = SparkSession.builder()
                .getOrCreate()
                .sqlContext();
    }

    public void getTopTenPopularWords() {

        Dataset<Row> set = Helper.readData(sqlContext, "Posts.xml");

        set.select(explode(split(col("_Body")," "))
                .alias("Word"))
                .where(col("Word")
                .notEqual(""))
                .groupBy("Word")
                .count()
                .alias("Count")
                .orderBy(desc("Count"))
                .show(10);
    }
}
