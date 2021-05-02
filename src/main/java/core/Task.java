package core;

import org.apache.spark.sql.SparkSession;


public class Task {
	
		static SparkSession session = SparkSession.builder().appName("Java Spark SQL basic example").master("local[*]").getOrCreate();
		
		static String output_dir;

	public static void main (String[] args) {
		
		String datasets = args[0];
		
		output_dir = args[1];
		
		
		session.read().format("csv").option("header", "true").load(datasets).withColumnRenamed("Track Name","Song").createOrReplaceTempView("dataset");
		
		MaxArtist();
		
		Joinspark();
	}
	
	public static void MaxArtist() {
		
		//Raggruppo in base all'artista e vedo quante su canzoni sono presenti nel dataset
		
		session.sql("SELECT Artist, COUNT(Song) AS Canzoni "
				+ "FROM dataset "
				+ "GROUP BY Artist ").createOrReplaceTempView("Group");
		
		//Trovo l'artista con il maggior numero di canzoni nel dataset
		
		session.sql("SELECT * "
				+ "FROM Group "
				+ "WHERE Canzoni = (SELECT MAX(Canzoni) FROM Group) ").createOrReplaceTempView("MaxSong");
		
		session.sql("SELECT dataset.* "
				+ "FROM MaxSong JOIN dataset ON MaxSong.Artist = dataset.Artist ").write().option("sep", ";").csv(output_dir + "/MaxArtist-output");
		
	}
		
	public static void Joinspark() {
		session.sql("SELECT Position, Song, Artist, Streams "
				+ "FROM dataset ").createOrReplaceTempView("Tab1");
		
		session.sql("SELECT Song, URL, Date, Region "
				+ "FROM dataset ").createOrReplaceTempView("Tab2");
		
		session.sql("SELECT Tab1.*, TAb2.URL, Tab2.Date, Tab2.Region "
				+ "FROM Tab1 JOIN Tab2 "
				+ "ON Tab1.Song = Tab2.Song "
				+ "WHERE Artist=\"Muse\" "
				+ "OR Artist=\"Nirvana\" "
				+ "OR Artist=\"Michael Jackson\" "
				+ "OR Artist LIKE \"%Beatles\" ").write().option("sep", ";").csv(output_dir + "/output-spark");	
	}
}
