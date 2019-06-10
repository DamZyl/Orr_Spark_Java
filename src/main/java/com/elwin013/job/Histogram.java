package com.elwin013.job;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.style.Styler;

public class Histogram {

    public CategoryChart getChart(String title, double[] dataX, double[] dataY ) {

        CategoryChart chart = new CategoryChartBuilder()
                .width(800)
                .height(600)
                .title("Histogram")
                .xAxisTitle("Data")
                .yAxisTitle("Result")
                .build();

        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        chart.addSeries(title, dataX, dataY);

        return chart;
    }
}
