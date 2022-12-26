package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFramesBasics extends App{

  val spark = SparkSession.builder()
            .appName("Dataframe Basics")
    .config("spark.master","local")
            .getOrCreate()
}
