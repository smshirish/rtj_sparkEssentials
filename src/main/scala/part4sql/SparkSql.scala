package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part1recap.SparkUtils
import part2dataframes.JoinsExercises
import part2dataframes.JoinsExercises._

object SparkSql extends App{

  val sparkDBBasePath = "src/main/resources/warehouse"

  val spark = SparkSession.builder()
    .appName("SparkSQL")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir",sparkDBBasePath)
    //option below commented out as it does not work .Not a show stopped as one time transfer stil possible
  //  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")  //Spark 2 only hack to allow table override
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

  val empDF = SparkUtils.readTable(spark,"employees")
  empDF.show()

  /**
    * the tansfer below works only if existing DB table directories are deleted each time.
    * "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true""  does not work
    **/

  SparkUtils.transferTables(spark, sparkDBBasePath, List(
    "departments",
    "dept_emp",
    "dept_manager",
    "employees",
    "movies",
    "salaries",
    "titles"))


  //read from spark tables
  val emp2DF = spark.read.table("employees")
  emp2DF.show()

}
