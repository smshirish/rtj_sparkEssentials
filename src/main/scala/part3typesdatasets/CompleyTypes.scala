package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
object CompleyTypes extends App {

  val spark = SparkSession.builder()
    .appName("Cpompley types")
    .config("spark.master","local")
    .getOrCreate()

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  ///moviesDF.show()
  /**
    * Dates
    */
  //casting string to data type from custom date format and doing some date magic
  //date arthimatic with datediff, date_add, date_sub

  val moviesWithReleaseDateDF =  moviesDF.select(col("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("ActualReleaseDate") )
    .withColumn("Today", current_date())
    .withColumn("Now", current_timestamp())
    .withColumn("Movie_age", datediff(col("Today"),col("ActualReleaseDate" ))/365)

  moviesWithReleaseDateDF.show(300,false)

  /**
    * Exercise:
    * 1:How do we deal with multiple date formats -> Parse one by one and use nulls to decide which dates need to be parsed with anoher format
    * 2:Read the stocks DF and parse dates correctly
    *
    */

   //exercise 2:Read the stocks DF and parse dates correctly
  val stockSchema = StructType(Array(
    StructField("symbol",StringType)
    ,StructField("date",DateType)
    ,StructField("price",DoubleType)
  ))

  val stocksDF = spark.read
    .schema(stockSchema)
    .option("header","true")
    .option("nullValue","")
    .option("sep",",")
    .option("dateFormat","MMM dd YYYY")
    .csv("src/main/resources/data/stocks.csv")

  ///stocksDF.show(false)
  ///stocksDF.printSchema()

  //second soluction , without schema
  val stocksDF2 = spark.read.format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .option("nullValue","")
    .option("sep",",")
    .load("src/main/resources/data/stocks.csv")

  val stockDF2WithActualDate = stocksDF2.withColumn("actual_date",to_date(col("date"),"MMM dd yyyy"))
  ///stockDF2WithActualDate.show(300, false)

  /**
    * Structures
    */
  //Struct with col operators
  moviesDF.select( col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    ///.show(100,false)

  //strictures with expr
  moviesDF.selectExpr("Title", " (US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title","Profit.US_Gross as US_Profit2")
    //.show(100)
}
