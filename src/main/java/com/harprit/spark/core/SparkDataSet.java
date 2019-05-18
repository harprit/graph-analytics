package com.harprit.spark.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataSet {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

		Dataset<Row> numbersDf = sparkSession.range(1000).toDF("number");
		numbersDf.show();
		
		// transformation
		Dataset<Row> evenNumbersDf = numbersDf.where("number % 2 == 0");
		evenNumbersDf.show();
		
		// action
		System.out.println(evenNumbersDf.count());
	}
}
