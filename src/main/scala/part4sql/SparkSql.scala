package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part2dataframes.JoinsExercises
import part2dataframes.JoinsExercises._

object SparkSql extends App{

  val spark = SparkSession.builder()
    .appName("SparkSQL")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.printSchema()

  //DF select
  carsDF.select(col("Name")).where(  col("Origin") === "USA")
    .show()

  //Spark SQL
  carsDF.createTempView("Cars")

  val americanCarsDF = spark.sql(
    """
      | select Name from Cars where Origin = 'USA'
      |""".stripMargin)

  /// americanCarsDF.show()

  spark.sql(" create database rtjvm ")
  spark.sql(" use rtjvm")
  val dataBases = spark.sql(" show databases ")
 /// dataBases.show()

  val empDF = JoinsExercises.readTableAsDataframe2(spark,"employees")
  empDF.show()
}
