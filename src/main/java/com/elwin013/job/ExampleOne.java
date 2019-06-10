package com.elwin013.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ExampleOne {

    private final SQLContext sqlContext;

    public ExampleOne() {

        sqlContext = SparkSession.builder()
                .getOrCreate()
                .sqlContext();
    }

    public void getTopTenPopularTags() {

        Dataset<Row> set = Helper.readData(sqlContext, "Tags.xml");

        set.createOrReplaceTempView("Set");

        sqlContext.sql("Select _TagName as Name, _Count as Count" +
                " From Set Order By Count desc").show(10);
    }
}
