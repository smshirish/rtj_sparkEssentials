package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp {

  def main (args: Array[String]) : Unit = {
    /**
      * Movies json input file name as arg(0)
      * GoodComedies.json -> output  file name as arg(1)
      * Good comedies = Genre == Comedy and IMDB_Rating > 6.5
      *
      */

    if(args.length != 2){
      println("Need input path and output path")
      System.exit(1)
    }
    val moviesJsonFilePath = args(0)

    val spark = SparkSession.builder()
      .appName("Test deployment of Spark App")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema","true")
      .json(moviesJsonFilePath)

    val goodComediesDF = moviesDF.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    ).where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .orderBy(col("Major_Genre").desc_nulls_last)

    goodComediesDF.show()

    val goodComediesJsonOutputFilePath = args(1)
    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(goodComediesJsonOutputFilePath)
  }

}
