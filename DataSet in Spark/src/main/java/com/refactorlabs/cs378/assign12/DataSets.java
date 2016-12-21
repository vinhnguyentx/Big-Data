/**
TO RUN:
./bin/spark-submit --class com.refactorlabs.cs378.assign12.DataSets
--master local[1] /Users/vinhnguyen/Dropbox/cs378/Assign12/target/bdp-spark-0.12.jar
/Users/vinhnguyen/Downloads/dataSet12.csv
/Users/vinhnguyen/Dropbox/cs378/Assign12/output/outMake
/Users/vinhnguyen/Dropbox/cs378/Assign12/output/outYear
/Users/vinhnguyen/Dropbox/cs378/Assign12/output/outVin
**/
package com.refactorlabs.cs378.assign12;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class DataSets {

    public static void main(String[] args) throws AnalysisException{
        Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

        String inputFilename = args[0];

        String makeOutput = args[1];
        String yearOutput = args[2];
        String vinOutput = args[3];

        // Create a Java Spark context
        SparkConf conf = new SparkConf().setAppName(DataSets.class.getName()).setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Create a Spark context
        SparkContext sc = jsc.sc();

        SparkSession session = SparkSession.builder().appName("DataSets").config(conf).getOrCreate();

        // Set up the table for all events
        Dataset<Row> df = session.read().option("header", "true").csv(inputFilename);
        df.createTempView("allEvents");

        // Select required info for make/model
        Dataset<Row> df_price_raw = session.sql("SELECT DISTINCT make, model, vin, price FROM allEvents WHERE price > 0 ").as("df_price_raw");
        df_price_raw.createTempView("df_price_raw");
        // Query against the table
        Dataset<Row> df_price_final = session.sql("SELECT make, model, MIN(price), MAX(price), AVG(price) FROM df_price_raw GROUP BY make, model ORDER BY make, model");
        // Print out result
        df_price_final.repartition(1).write().option("header", "true").csv(makeOutput);

        // Select required info for year
        Dataset<Row> df_mileage_raw = session.sql("SELECT DISTINCT year, vin, mileage FROM allEvents WHERE mileage > 0").as("df_mileage_raw");
        df_mileage_raw.createTempView("df_mileage_raw");
        // Query against the table
        Dataset<Row> df_mileage_final = session.sql("SELECT year, MIN(mileage), MAX(mileage), AVG(mileage) FROM df_mileage_raw GROUP BY year ORDER BY year");
        // Print out result
        df_mileage_final.repartition(1).write().option("header", "true").csv(yearOutput);

        // Select required info for VIN
        Dataset<Row> df_event_raw = session.sql("SELECT vin, SUBSTRING(event, 1, LOCATE(' ', event, 1) - 1) AS event_type, timestamp FROM allEvents").as("df_event_raw");
        df_event_raw.createTempView("df_event_raw");
        // Query against the table
        Dataset<Row> df_event_final = session.sql("SELECT vin, event_type, COUNT(timestamp) FROM df_event_raw GROUP BY vin, event_type ORDER BY vin, event_type");
        // Print out result
        df_event_final.repartition(1).write().option("header", "true").csv(vinOutput);

        jsc.stop();
    }
}
