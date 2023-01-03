package part2dataframes

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object AggregationsAndGrouping extends App{

  val spark = SparkSession.builder()
    .appName("AggregationsAndGrouping")
    .config("spark.master","local")

    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  //moviesDF.show

  //count all the rows in the DF, including nulls
  private val value: Dataset[Long] = moviesDF.select(count("*"))
  value.show()
  private val val2: DataFrame = moviesDF.select(count(col("Major_Genre"))) //all rows with major_genre
  val2.show()
  private val val3: DataFrame = moviesDF.selectExpr("count(Major_Genre)")
  val3.show()

  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  //approx_count_distinct is faster but not accurate
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  //min and max
  moviesDF.select(min(col("IMDB_Rating"))).show()
  moviesDF.selectExpr("min(IMDB_Rating)").show()

  //sum and
  moviesDF.select(sum(col("US_Gross"))).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  //avg
  moviesDF.select(avg(col("US_Gross"))).show()

  //data scienct
  moviesDF.select(mean(col("Rotten_Tomatoes_Rating")),stddev(col("Rotten_Tomatoes_Rating"))).show()

  //grouping
  moviesDF.groupBy(col("Major_Genre")) //includes null
    .count().show()

  val avgRatingBygenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg(("IMDB_Rating"))

  val aggregationsBygenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies")
      ,avg("IMDB_Rating").as("Avg_rating")
    ).orderBy("Avg_rating")
  aggregationsBygenreDF.show

  /**
    * Exercises :
    * 1) Sum up all the profits of all the movies in the DF
    * 2)Count how many distinct directors we have
    * 3)Show mean and standard deviation of US gross revenue for the movies
    * 4)Comppute the avg IMDB rating and AVG US gross revenue PER DIRECTOR
    */

  //|       Creative_Type|         Director|   Distributor|IMDB_Rating|
  // IMDB_Votes|MPAA_Rating|Major_Genre|Production_Budget|
  // Release_Date|Rotten_Tomatoes_Rating|Running_Time_min|
  // Source|               Title|US_DVD_Sales|US_Gross|Worldwide_Gross|
  moviesDF.show
  val profitsOfAllMoviesDF = moviesDF.select( ((col("US_Gross") + col("Worldwide_Gross"))).as("Net_sales"))
    .select(sum(col("Net_sales")))
    //profitsOfAllMoviesDF.show()

  val countOfDistinctDirectorsDF = moviesDF.select(countDistinct(col("Director")))
  //countOfDistinctDirectorsDF.show

  val meanAndStdDevDF = moviesDF.select(mean(col("US_Gross")),stddev(col("US_Gross")))
  meanAndStdDevDF.show

  moviesDF.groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("avg_rating")
      ,avg(col("US_Gross"))
    ).orderBy(col("avg_rating").desc_nulls_last).show
}
