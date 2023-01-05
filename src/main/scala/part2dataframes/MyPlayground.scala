package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import part1recap.SparkUtils
object MyPlayground extends App{

  val spark = SparkSession.builder()
    .appName("C_3_SparkSqlExercises")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  import spark.implicits._
  /**
  val empDFParquet = spark.read.parquet("src/main/resources/warehouse/rtjvm.db/employees")
  empDFParquet.printSchema()
  empDFParquet.createTempView("employees")
**/
 // SparkUtils.transferTables(spark,SparkUtils.SPARK_DB_PATH,List("employees"),false)

 val carsDF = spark.read
   .option("inferSchema","true")
   .json("src/main/resources/data/cars.json")

  // to_date(col("Release_Date"),"dd-MMM-yy").as("ActualReleaseDate")
  //val carsWithYearDF = carsDF.select(col("Name"), to_date(col("Year"),"yyyy-MM-dd").as("ActualYear"), col("Origin")).orderBy(col("ActualYear").desc_nulls_last)

  //carsWithYearDF.select(col("Name"),col("ActualYear") ).where((col("ActualYear") > "1971-01-01") and (col("ActualYear") < "1976-01-01")).show(500)
  ///carsWithYearDF.show(500)


 // val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA").show()
 val empDF = SparkUtils.readTable(spark,"employees")// .where((col("hire_date") > "1985-01-01") and (col("hire_date") < "1988-01-01") )
  empDF.show()
  empDF.createOrReplaceTempView("employees")


  /**
    * Exercise2: How many employees were hired between jan 1 2000 and jan 1 2001.
    */
  spark.sql(" create database rtjvm ")
  spark.sql(" use rtjvm")
  val df = spark.sql(" select * from employees where  hire_date > '1985-01-01' and hire_date <  '1988-01-01' ")
  df.show()

}
