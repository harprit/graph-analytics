package com.harprit.spark.revise;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHelloWorld2 {

	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
		
		Dataset<Row> flightsData = sparkSession.read().option("inferSchema", "true").option("header", "true").csv("src/main/resources/2015-summary.csv");
		
		flightsData.show();
		flightsData.createOrReplaceTempView("flightsData");
		
		// dataframe vs SQL examples follow:-
		
		// sorting
		flightsData.sort("ORIGIN_COUNTRY_NAME").show();
		sparkSession.sql("select * from flightsData order by ORIGIN_COUNTRY_NAME asc").show();
		
		// group by
		flightsData.groupBy("DEST_COUNTRY_NAME").count().show();
		sparkSession.sql("select DEST_COUNTRY_NAME, count(1) from flightsData group by DEST_COUNTRY_NAME");

	}

}
