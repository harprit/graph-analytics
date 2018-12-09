package com.harprit.spark.graph;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

public class UserGuideExamples {
	
	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
		
		Dataset<Row> e = sparkSession.read().option("inferSchema", "true").option("header", "true").csv("src/main/resources/friends.csv");
		
		Dataset<Row> v = sparkSession.read().option("inferSchema", "true").option("header", "true").csv("src/main/resources/relations.csv");
		
		GraphFrame g = new GraphFrame(e, v);
		
		g.vertices().show();
		g.edges().show();
		
		// need graphx lib
		// Dataset<Row> vertexInDegrees = g.inDegrees();
		
		// Find the youngest user's age in the graph.
		// This queries the vertex DataFrame.

		// g.vertices().groupBy().min("age").show();
		
		// Count the number of "follows" in the graph.
		// This queries the edge DataFrame.
		// System.out.println(g.edges().filter("relationship = 'follow'").count());
		
		// Label Propagation Algorithm (LPA)
		
		Dataset<Row> result = g.labelPropagation().maxIter(5).run();
		result.select("id", "label").show();
	}
}
