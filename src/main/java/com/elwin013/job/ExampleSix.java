package com.elwin013.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class ExampleSix {

    private final SQLContext sqlContext;

    public ExampleSix() {

        sqlContext = SparkSession.builder()
                .getOrCreate()
                .sqlContext();
    }

    public void getTimetable() {

        Dataset<Row> set = Helper.readData(sqlContext, "Comments.xml");

        set = set.withColumn("Date", split(col("_CreationDate"), "T")
                .getItem(0)
                .cast(DateType))
                .withColumn("Time", split(col("_CreationDate"), "T")
                        .getItem(1)
                        .cast(TimestampType));

        set = set.withColumn("Hour", hour(col("Time")));

        set.show(10);
        set.createOrReplaceTempView("Set");

        Object[] dataToHistogram = sqlContext.sql("Select Hour, Count(*) as Count" +
                " From Set Where _Score > 5 Group By Hour Order By Hour asc")
                .collectAsList()
                .toArray();

        double[] dataX = new double[24];
        double[] dataY = new double[24];

        Helper.writeDataToVariable(dataToHistogram, dataX, dataY);

        sqlContext.sql("Select Hour, Count(*) as Count" +
                " From Set Where _Score > 5 Group By Hour Order By Hour asc")
                .show(24);

        Helper.drawHistogram(dataX, dataY);
    }
}
