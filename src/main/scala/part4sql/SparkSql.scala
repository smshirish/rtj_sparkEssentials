package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part2dataframes.JoinsExercises
import part2dataframes.JoinsExercises._

object SparkSql extends App{

  val spark = SparkSession.builder()
    .appName("SparkSQL")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
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

  val empDF = JoinsExercises.readTable2(spark,"employees")
  ///empDF.show()

  /**
    * This table is created below again, so commenting out as overwrite option not working with spark3.0

  empDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")
  **/

  def transferTables(tableNames:List[String]) =  tableNames.foreach{ tableName =>
      val tableDF = JoinsExercises.readTable2(spark,tableName)
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }

  /**
    * the tansfer below works only once as the option overWrite existing tables does not work
    * "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true""  does not work
    */
  transferTables(List(
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
