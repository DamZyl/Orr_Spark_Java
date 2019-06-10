package com.elwin013;

import com.elwin013.job.*;
import org.apache.spark.SparkConf;

import com.elwin013.configuration.AppProperties;
import com.elwin013.configuration.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

public class Application {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {

        // Initialize configuration
        final SparkConf sparkConf = new SparkConf();
        final AppProperties properties = new Configuration().getProperties();

        sparkConf.setMaster("local")
                .setAppName("AppName")
                .set("spark.executor.instances", properties.getSparkExecutorInstances())
                .set("spark.executor.cores", properties.getSparkExecutorCores());

        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //ExampleOne exampleOne = new ExampleOne();
        //exampleOne.getTopTenPopularTags();

        //ExampleTwo exampleTwo= new ExampleTwo();
        //exampleTwo.getTopTenPopularWords();

        //ExampleThree exampleThree = new ExampleThree();
        //exampleThree.getTimetableIn2017();

        //ExampleFour exampleFour = new ExampleFour();
        //exampleFour.getTopTenUserWithLongestAnswers();

        //ExampleFive exampleFive = new ExampleFive();
        //exampleFive.getTopTenUserWithHighestVote();

        ExampleSix exampleSix = new ExampleSix();
        exampleSix.getTimetable();

        sc.close();
    }
}
