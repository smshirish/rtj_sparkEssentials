package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFramesBasics extends App{

  //create spark session
  val spark = SparkSession.builder()
            .appName("Dataframe Basics")
    .config("spark.master","local")
            .getOrCreate()

  //read data frame

  spark.read.format("json").load("")
}
