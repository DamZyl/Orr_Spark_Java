package com.elwin013.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.SwingWrapper;

public class Helper {

    public static Dataset<Row> readData(SQLContext sqlContext, String input) {

        return sqlContext.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "row")
                .load("src" + "/main/resources/" + input);
    }

    public static void writeDataToVariable(Object[] dataToHistogram, double[] dataX, double[] dataY) {

        for (int i = 0; i < 24; i++) {

            if (i < 10) {

                dataY[i] = Double.parseDouble(dataToHistogram[i]
                        .toString()
                        .substring(3, dataToHistogram[i].toString().length() - 1));
            }

            else {

                dataY[i] = Double.parseDouble(dataToHistogram[i]
                        .toString()
                        .substring(4, dataToHistogram[i].toString().length() - 1));
            }

            dataX[i] = i;
        }
    }

    public static void drawHistogram(double[] dataX, double[] dataY) {

        Histogram histogramXChart = new Histogram();
        CategoryChart chart =  histogramXChart.getChart("Histogram", dataX, dataY);
        new SwingWrapper(chart).displayChart();
    }
}
