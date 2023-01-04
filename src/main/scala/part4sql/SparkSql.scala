package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSql extends App{

  val spark = SparkSession.builder()
    .appName("SparkSQL")
    .config("spark.master","local")
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
      | select * from Cars where Origin = 'USA'
      |""".stripMargin)

  americanCarsDF.show()
}
