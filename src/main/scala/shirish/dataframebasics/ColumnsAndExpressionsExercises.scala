package shirish.dataframebasics

import org.apache.spark.sql.SparkSession

object ColumnsAndExpressionsExercises extends App {

  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  /*
  root
   |-- Creative_Type: string (nullable = true)
   |-- Director: string (nullable = true)
   |-- Distributor: string (nullable = true)
   |-- IMDB_Rating: double (nullable = true)
   |-- IMDB_Votes: long (nullable = true)
   |-- MPAA_Rating: string (nullable = true)
   |-- Major_Genre: string (nullable = true)
   |-- Production_Budget: long (nullable = true)
   |-- Release_Date: string (nullable = true)
   |-- Rotten_Tomatoes_Rating: long (nullable = true)
   |-- Running_Time_min: long (nullable = true)
   |-- Source: string (nullable = true)
   |-- Title: string (nullable = true)
   |-- US_DVD_Sales: long (nullable = true)
   |-- US_Gross: long (nullable = true)
   |-- Worldwide_Gross: long (nullable = true)
   */

  /**
   * Exercises :
   * Read movies DF and select 2 columns of your choice ( Title, Major_Genre, Rotten_Tomatoes_Rating, IMDB_Rating
   * Prepare a column summing total profit for movies = US_Gross + Worldwide_Gross  + US_DVD_Sales
   */

  val moviesDF = spark.read.format("json").option("inferSchema", "true").load(PATH + "movies.json")
  //moviesDF.printSchema()
  //moviesDF.show()

  //exercise 1: Select columns Title, Major_Genre, Rotten_Tomatoes_Rating, IMDB_Rating
  import org.apache.spark.sql.functions._
  //Solution 1: select2 columns
  //Select with string
  moviesDF.select("Title","Major_Genre")
  //  .show()

  //select with expressions
  moviesDF.select(col("Title"), expr("Major_Genre"), moviesDF.col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"))
    //.show()

  //solution 1:version 3 , with selectExpr
  moviesDF.selectExpr("Title","Major_Genre")
  ///  .show()


  val moviesDFWithGross =   moviesDF.select(col("Title"), expr("Major_Genre"), moviesDF.col("Rotten_Tomatoes_Rating"), col("IMDB_Rating")
  ,col("US_Gross"), col("Worldwide_Gross") , col("US_DVD_Sales"), col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")
    // US_Gross + Worldwide_Gross  + US_DVD_Sales
  )
  /// moviesDFWithGross.show(100)

  val grossExpr = (moviesDF.col("US_Gross") + moviesDF.col("Worldwide_Gross") + moviesDF.col("Worldwide_Gross")).as("NET_GROSS")
  val moviesDFWithGross2  = moviesDF.select(col("Title"), expr("Major_Genre"), moviesDF.col("Rotten_Tomatoes_Rating"), col("IMDB_Rating")
    , col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales"),
    grossExpr// US_Gross + Worldwide_Gross  + US_DVD_Sales
  )
///  moviesDFWithGross2.show(1000)

  val moviesDFWithGross3  = moviesDF.selectExpr("Title","US_Gross","Worldwide_Gross","US_DVD_Sales","US_DVD_Sales + Worldwide_Gross + US_Gross as NetGross" )
 /// moviesDFWithGross3.show

  //select all COMEDY movies with imdb rating of above 6 : Using chained filters
  val comedyMoviesDF = moviesDF.select("Title","Major_Genre","IMDB_Rating").filter(col("Major_Genre") === "Comedy").filter(col("IMDB_Rating") > 6).orderBy(col("IMDB_Rating"))
    .show(1000)

  //select all COMEDY movies with imdb rating of above 6 : Using AND
  val comedyMoviesDF2 = moviesDF.select("Title","Major_Genre","IMDB_Rating").filter((col("Major_Genre") === "Comedy").and(col("IMDB_Rating") > 6)).orderBy(col("IMDB_Rating").desc)
  ///  .show(1000)

  //select all COMEDY movies with imdb rating of above 6 : Using String expression
  val comedyMoviesDF3 = moviesDF.select("Title","Major_Genre","IMDB_Rating").filter(" Major_Genre = 'Comedy' AND  IMDB_Rating > 6").orderBy(col("IMDB_Rating").desc)
   // filter((col("Major_Genre") === "Comedy").and(col("IMDB_Rating") > 6)).orderBy(col("IMDB_Rating").desc)
  ///  .show(1000)

}
