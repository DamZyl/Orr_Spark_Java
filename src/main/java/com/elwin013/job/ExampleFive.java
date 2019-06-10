package com.elwin013.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ExampleFive {

    private final SQLContext sqlContext;

    public ExampleFive() {

        sqlContext = SparkSession.builder()
                .getOrCreate()
                .sqlContext();
    }

    public void getTopTenUserWithHighestVote() {

        Dataset<Row> setOne = Helper.readData(sqlContext, "Comments.xml");
        Dataset<Row> setTwo = Helper.readData(sqlContext, "Users.xml");

        Dataset<Row> joinSet = setOne.join(setTwo, setOne.col("_UserId")
                .equalTo(setTwo.col("_Id")));

        joinSet.createOrReplaceTempView("Set");

        sqlContext.sql("Select _DisplayName as User, avg(_Score) as Avg" +
                " From Set Group By _DisplayName Order By Avg desc").show(10);
    }
}
