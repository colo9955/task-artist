package core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Task {

	public static void main (String args[]) {
		
		SparkSession session = SparkSession.builder().appName("Java Spark SQL basic example").master("local[*]").getOrCreate();
		
		session.read().format("csv").option("header", "true").load("/Users/filippocolombo/Downloads/Spotify.csv").withColumnRenamed("Track Name","Song").createOrReplaceTempView("dataset");
		
		//session.sql("SELECT * FROM dataset WHERE artist=\"Muse\"").write().csv("/Users/filippocolombo/Downloads/sparkoutput");
		
		//Raggruppo in base all'artista e vedo quante su canzoni sono presenti nel dataset
		
		session.sql("SELECT Artist, COUNT(Song) AS Canzoni FROM dataset GROUP BY Artist").createOrReplaceTempView("Somma");
		
		//Trovo l'artista con il maggior numero di canzoni nel dataset
		
		session.sql("SELECT * FROM dataset WHERE Canzoni = (SELECT MAX(Canzoni) FROM Somma)").show();
		
		//session.sql("SELECT Position, Song, Artist, Streams FROM dataset ").createOrReplaceTempView("Tab1");
		
		//session.sql("SELECT Song AS Canzone, URL, Date, Region FROM dataset ").createOrReplaceTempView("Tab2");
		
		//session.sql("SELECT * FROM Tab1 JOIN Tab2 ON Song = Canzone WHERE Artist=\"Muse\"").write().option("sep", ";").csv("/Users/filippocolombo/Downloads/sparkoutput");	
	}

}
