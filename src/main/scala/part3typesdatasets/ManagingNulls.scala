package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App{

  val spark = SparkSession.builder()
    .appName("ManagingNulls")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //select first not null value with coalesce
  moviesDF.select(col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating")*10,
    coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating")*10)
  )
  ///  .show()

  //checking for nulls isNull / isNotNull
  moviesDF.select(col("Title"),
    col("Rotten_Tomatoes_Rating")
  ).where( col("Rotten_Tomatoes_Rating").isNull)
   /// .show()

  //nulls when ordering
  moviesDF.select("*").orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_first)
   // .show()

  //dropping nulls
  moviesDF.select("*").na.drop()
    .show()
}
