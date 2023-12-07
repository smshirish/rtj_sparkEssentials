package shirish.sparktypesanddatasets

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._
object ComplexTypes extends App {
  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

  val moviesDF = spark.read.option("inferSchema", "true").json(PATH + "movies.json")

  //date processing
  val moviesWithReleaseDate = moviesDF.select(col("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_release_date") )

  moviesWithReleaseDate.withColumn("Today", current_date())
    .withColumn("Now",current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_release_date"))/365)  //date_add , date_sub
  ///.show

  //Dates and formating issues , null values
  moviesWithReleaseDate.select("*").where(col("Actual_release_date").isNull)  //55 Days at Peking is null because the date format is wrong.
  ///  .show()

  /**
   * Exercise: How do we deal with multiple date formats  -> parse DF multiple times and union the DF
   * Read stocks json and format date
   */

  val stocksDF = spark.read.option("inferSchema", "true").option("header","true").csv(PATH + "stocks.csv")

  stocksDF.select(col("symbol"), col("date"), to_date(col("date"), "MMM dd yyyy"))
    ///.show()

  //Structures  and selecting individual field from a struct
  //version 1:With col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"),col("Profit"), col("Profit").getField("US_Gross").as("US_Profit") )
  ///  .show
  //version 2 structs:With selectExpr
  moviesDF.selectExpr("Title"," (US_Gross,Worldwide_Gross) as Profit")
    .selectExpr("Title","Profit", "Profit.US_Gross")
   /// .show()  //,"Profit.US_Gross as US_Profit"

  //arrays
  val moviesWithTitleWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) //Title_Words is an array of strings
    ///.show()

  //array operations
  moviesWithTitleWords.select(col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"),"Love")
  )
    .show



}
