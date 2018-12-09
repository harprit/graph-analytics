package com.harprit.spark.graph;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import static org.apache.spark.sql.functions.desc;

public class HelloSparkGraphWorld {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

		Dataset<Row> bikeStations = sparkSession.read().option("header", "true").csv("src/main/resources/201508_station_data.csv");

		Dataset<Row> tripData = sparkSession.read().option("header", "true").csv("src/main/resources/201508_trip_data.csv");

		bikeStations.show();
		tripData.show();

		// applying naming conventions of GraphFrames library

		// vertices table need vertices names as id
		Dataset<Row> stationVertices = bikeStations.withColumnRenamed("name", "id").distinct();

		// edges table needs each edge's source vertex as src and destination vertex as dst
		Dataset<Row> tripEdges = tripData.withColumnRenamed("Start Station", "src").withColumnRenamed("End Station", "dst");

		GraphFrame stationGraph = new GraphFrame(stationVertices, tripEdges);
		stationGraph.cache();

		// System.out.println(stationGraph.vertices().count());
		// System.out.println(stationGraph.edges().count());
		// System.out.println(tripData.count());

		stationGraph.edges().groupBy("src", "dst").count().orderBy(desc("count")).show();

		stationGraph.edges().where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'").groupBy("src", "dst").count().orderBy(desc("count")).show(10);

		// Sub graphs
		Dataset<Row> townAnd7thEdges = stationGraph.edges().where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'");
		GraphFrame subgraph = new GraphFrame(stationGraph.vertices(), townAnd7thEdges);
		System.out.println(subgraph.vertices().count());
		System.out.println(subgraph.edges().count());
	}
}
