package com.elwin013.job;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;


public class ExampleFour {

    private final SQLContext sqlContext;

    public ExampleFour() {

        sqlContext = SparkSession.builder()
                .getOrCreate()
                .sqlContext();
    }

    public void getTopTenUserWithLongestAnswers() {

        Dataset<Row> setOne = Helper.readData(sqlContext, "Comments.xml");
        Dataset<Row> setTwo = Helper.readData(sqlContext, "Users.xml");

        Dataset<Row> joinSet = setOne.join(setTwo, setOne.col("_UserId")
                .equalTo(setTwo.col("_Id")));

        joinSet = joinSet.withColumn("Words",
                size(split(joinSet.col("_Text"), " ")));

        joinSet.createOrReplaceTempView("Set");

        sqlContext.sql("Select _DisplayName as User, Avg(Words) as Avg" +
                " From Set Group By _DisplayName Order By Avg desc").show(10);
    }
}
