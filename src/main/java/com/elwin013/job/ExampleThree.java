package com.elwin013.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DateType;

public class ExampleThree {

    private final SQLContext sqlContext;

    public ExampleThree() {

        sqlContext = SparkSession.builder()
                .getOrCreate()
                .sqlContext();
    }

    public void getTimetableIn2017() {

        Dataset<Row> set = Helper.readData(sqlContext, "Posts.xml");

        set = set.withColumn("Date", split(col("_CreationDate"),"T")
                .getItem(0)
                .cast(DateType))
                .withColumn("Time", split(col("_CreationDate"),"T")
                .getItem(1));

        set = set.withColumn("Year", year(col("Date")))
                .withColumn("Hour", hour(col("Time")));

        set.createOrReplaceTempView("Set");

        Object[] dataToHistogram = sqlContext.sql("Select Hour, Count(*) as Count" +
                " From Set Where Year == 2017 Group By Hour Order By Hour asc")
                .collectAsList()
                .toArray();

        double[] dataX = new double[24];
        double[] dataY = new double[24];

        Helper.writeDataToVariable(dataToHistogram, dataX, dataY);

        sqlContext.sql("Select Hour, Count(*) as Count" +
                " From Set Where Year == 2017 Group By Hour Order By Count desc")
                .show(24);

        Helper.drawHistogram(dataX, dataY);
    }
}
