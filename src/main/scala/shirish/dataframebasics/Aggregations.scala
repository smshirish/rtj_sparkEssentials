package shirish.dataframebasics

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
object Aggregations extends App {


  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema","true").json(PATH+"movies.json")
  ///moviesDF.show

  /**
   * Counting
   */

  moviesDF.select(count(col("*"))) //all the values including Nulls
 ///  .show()

  moviesDF.select(count(col("Major_Genre"))) //all the values except Nulls
  /// .show()
  moviesDF.selectExpr("count(Major_Genre)")
  ///  .show()

  //count_distinct
  moviesDF.select(countDistinct(col("Major_Genre")))
 ///   .show()

  //approx count
  moviesDF.select(approx_count_distinct(col("Major_genre")))
    ///.show()
  moviesDF.printSchema()
  //min / max
  moviesDF.select(min(col("IMDB_Rating")))
  //  .show()
  moviesDF.selectExpr("min('IMDB_Rating')")
  ///  .show()

  //Sum
  moviesDF.select(sum(col("US_Gross")))
    //.show()
  moviesDF.selectExpr("sum('US_Gross')")
   /// .show()

  //avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")), mean(col("Rotten_Tomatoes_Rating")), stddev(col("Rotten_Tomatoes_Rating")))
   /// .show()

  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")).count()
 // countByGenreDF.show()

  val avgIMDBRatingByGenreDF = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")
  ///avgIMDBRatingByGenreDF.show

  //multiple aggregations

  val aggregationsByGenreDF = moviesDF.groupBy(col("Major_Genre")).agg(
    count("*").as("N_Movies"),
    avg("IMDB_Rating").as("Avg_Rating")
  )
 /// aggregationsByGenreDF.show

  /**
   * Sum of all profits of all movies
   * Count how many distinct directrs we have
   * Show the mean and std dev of US Gross revenue for the movies
   * Compare AVG IMDB rating and Avg Gross revenue Per director
   */

  moviesDF.select((sum(col("Worldwide_Gross") + col("US_Gross") + col("US_DVD_Sales"))).as("ALL_PROFITS"))
 ///   .show()

  moviesDF.select( (col("Worldwide_Gross") + col("US_Gross") + col("US_DVD_Sales")).as("total_profits"))
    .select(sum("total_profits"))
 // .show()

  moviesDF.select(countDistinct(col("Director")))
  //  .show()

  //men and std dev
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )
   // .show

  moviesDF.groupBy(col("Director")).agg(
    sum(col("US_Gross")).as("TotalUSGross"),
    avg(col("IMDB_Rating")).as("AvgRating")
  ).orderBy(col("AvgRating").desc_nulls_last)
    .show()

}
