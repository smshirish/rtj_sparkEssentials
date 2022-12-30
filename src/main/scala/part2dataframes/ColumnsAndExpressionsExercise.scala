package part2dataframes

import org.apache.spark.sql.SparkSession

object ColumnsAndExpressionsExercise extends App{

  val spark = SparkSession.builder().config("spark.master","local").appName("ColumnsAndExpressionsExercise").getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  /**
    * Read movies DF and select 2 columns of your choice
    * Create another column , summing up total profit of the movies.
    * Select all Comedy movies , with comedy rating above 6
    *
    */

  /// moviesDF.show()

  //super simple select
  val moviesCustomDF = moviesDF.select("Title","Distributor")
  ///moviesCustomDF.show
  moviesCustomDF.printSchema()
  //|       Creative_Type|         Director|   Distributor|IMDB_Rating|
  // IMDB_Votes|MPAA_Rating|Major_Genre|Production_Budget|
  // Release_Date|Rotten_Tomatoes_Rating|Running_Time_min|
  // Source|               Title|US_DVD_Sales|US_Gross|Worldwide_Gross|

  //use col and column
  import spark.implicits._
  import org.apache.spark.sql.functions.{col,column,expr}
  val moviesCustomDF2 = moviesDF.select(
    moviesDF.col("Title"), col("Distributor")
    , column("Director")
    ,'Creative_Type
    ,$"US_Gross"
    ,expr("Worldwide_Gross")
  )
  ///moviesCustomDF2.show


  val moviesCalculatedDF = moviesDF.select(
    col("Title")
    ,col("Director")
    ,col("US_Gross")
    ,col("Worldwide_Gross")
    ,expr("US_Gross + Worldwide_Gross").as("Net Earnings"))
  ///moviesCalculatedDF.show

  val moviesCalculatedDF2 = moviesDF.select(
    col("Title")
    ,col("Director")
    ,col("US_Gross")
    ,col("Worldwide_Gross")
    , (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Net_total_sales")
  )
  ///moviesCalculatedDF2.show

  val moviesCalculatedDFWithSelectExpr = moviesDF
    .selectExpr("Title", "Director", "US_Gross + Worldwide_Gross as total_gross")
  ///moviesCalculatedDFWithSelectExpr.show

  val moviesCalculatedDFWithColumn = moviesDF
    .select("Title", "Director","US_Gross","Worldwide_Gross").withColumn("total_gross2", col("US_Gross") + col("Worldwide_Gross") )
  moviesCalculatedDFWithColumn.show

  val comedyMoviesDF = moviesDF.filter(col("Major_Genre") === "Comedy")
  ///comedyMoviesDF.show

  val comedyMoviesDF2 = moviesDF.where(col("Major_Genre") === "Comedy")
  ///comedyMoviesDF2.show

  val comedyMoviesDF3 = moviesDF.where("Major_Genre = 'Comedy'")
  ///comedyMoviesDF3.show

  //|       Creative_Type|         Director|   Distributor|IMDB_Rating|
  // IMDB_Votes|MPAA_Rating|Major_Genre|Production_Budget|
  // Release_Date|Rotten_Tomatoes_Rating|Running_Time_min|
  // Source|               Title|US_DVD_Sales|US_Gross|Worldwide_Gross|


  val popularComedyMoviesDF = moviesDF.filter(
    (col("Major_Genre") === "Comedy")
      and (col("IMDB_Rating") > 6)
  )
  ///popularComedyMoviesDF.show

  val popularComedyMoviesDF2 = moviesDF.select("Title","Major_Genre","IMDB_Rating").where(
    (col("Major_Genre") === "Comedy")
      and col("IMDB_Rating") > 6
    )
 // popularComedyMoviesDF2.show

  val popularComedyMoviesDF3 = moviesDF.select("Title","Major_Genre","IMDB_Rating").where(
    "Major_Genre =  'Comedy' and IMDB_Rating > 6"
  )
  ///popularComedyMoviesDF3.show


  val popularComedyMoviesDF4 =  moviesDF.select("Title","Major_Genre","IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6 )
  popularComedyMoviesDF4.show

}

//.select("Title","Major_Genre","IMDB_Rating","Rotten_Tomatoes_Rating"  )