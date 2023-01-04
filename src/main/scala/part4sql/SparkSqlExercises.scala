package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import part1recap.SparkUtils

import java.io.File

object SparkSqlExercises extends App{

  val sparkDBBasePath = "src/main/resources/warehouse"
  val spark = SparkSession.builder()
    .appName("SparkSqlExercises")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir",sparkDBBasePath)
    .getOrCreate()

  /**
    * Exercises
    * 1:Read the movies DF and store it as a Spark table in rockthejvm DB
    */

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  spark.sql(" create database rtjvm ")
  spark.sql(" use rtjvm")
  //workaround ,must delete DB table dir if exists as overwrite option not supported with spark3.
  SparkUtils.deleteDBTableDir(sparkDBBasePath,"movies")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  val movies2DF = spark.read.table("movies")
  movies2DF.show()

}
