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
   /// .show()

  //dropping nulls
  moviesDF.select(col("Title"),col("Rotten_Tomatoes_Rating")).na.drop()
    ///.show()

  //replace nulls (fill all columns with same replacement)
  moviesDF.na.fill(0, List("Rotten_Tomatoes_Rating","IMDB_Rating"))
  ///  .show()

  //replace nulls , diff value for each column
  moviesDF.na.fill(Map(
    "Rotten_Tomatoes_Rating" -> 999,
    "IMDB_Rating" -> 11111,
    "Director" -> "Unknown")
  )
  ///  .show(300)

  //nulls , complex expressions
  moviesDF.selectExpr("Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull",  //same as coalesce above -> show Rotten_Tomatoes_Rating if not null , else imdb_Rating * 10
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl ", //same as above
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif " , // returns null if the 2 values are equal  , if two values are not equal, return first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10,0) as nvl2" //  if (first != null) second else third --->  if first argument not null, return second argument , otheriswe the third argument
  )
    .show(200)
}
