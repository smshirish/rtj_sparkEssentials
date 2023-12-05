package shirish.dataframebasics

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsExercises extends App {

  val PATH = "src/main/resources/data/"

  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()


  private def readDBTable(tableName: String): sql.DataFrame = {
    spark.read
      .format("jdbc")
      .option("user", "docker")
      .option("password", "docker")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("dbtable", tableName)
      .load()
  }

  /**
   * exercises
   * 1: Show all employees and their max salary
   * 2: show all emp who were never managers
   * 3:Job titls of best paid 10 employees in the company
   */

  // 1: Show all employees and their max salary
  val employeesDF = readDBTable("public.employees")
  val salariesDF = readDBTable("public.salaries")
  val maxSalariesDF = salariesDF.groupBy(col("emp_no")).max("salary")
  val empWithMaxSalariesDF_solution1 = employeesDF.join(maxSalariesDF, employeesDF.col("emp_no") === maxSalariesDF.col("emp_no"))
  empWithMaxSalariesDF_solution1.show()


  // 2: show all emp who were never managers
  val deptManagersDF = readDBTable("public.dept_manager")
  val empWhoWereNeverManagersDF_solution2 = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),"left_anti")
  empWhoWereNeverManagersDF_solution2.show()

  //3:Job titles of best paid 10 employees in the company

  val titlesDF = readDBTable("public.titles")

  //option 1:With column renames for duplicate columns
  /*
  *

  val latestEmploymentDF = salariesDF.withColumnRenamed("emp_no","emp_no_2").withColumnRenamed("from_date","from_date_2")
    .groupBy(col("emp_no_2")).agg(max("from_date_2").as("from_date_2")).orderBy(col("emp_no_2").desc_nulls_last)

  val latestSalaryDF = salariesDF.join(latestEmploymentDF, ( (salariesDF.col("emp_no") === latestEmploymentDF.col("emp_no_2"))
    and (salariesDF.col("from_date") === latestEmploymentDF.col("from_date_2"))))

 val top10Employees_solution3 =  latestSalaryDF.join(titlesDF.drop("from_date","to_date"), (latestSalaryDF.col("emp_no") === titlesDF.col("emp_no"))).orderBy(col("salary").desc_nulls_last).limit(10)
  top10Employees_solution3.show()

     */

  //option 2:Drop duplicate columns
  val latestEmploymentDF2 = salariesDF
    .groupBy(col("emp_no")).agg(max("from_date").as("from_date")).orderBy(col("emp_no").desc_nulls_last)

  val latestSalaryDF2 = salariesDF.join(latestEmploymentDF2, ((salariesDF.col("emp_no") === latestEmploymentDF2.col("emp_no"))
    and (salariesDF.col("from_date") === latestEmploymentDF2.col("from_date")))).drop(latestEmploymentDF2.col("emp_no")).drop(latestEmploymentDF2.col("from_date"))

  val top10Employees2_solution3 = latestSalaryDF2.join(titlesDF.drop("from_date","to_date"), (latestSalaryDF2.col("emp_no") === titlesDF.col("emp_no")))
    .drop(titlesDF.col("emp_no"))
    .orderBy(col("salary").desc_nulls_last).limit(10)
  top10Employees2_solution3.show()


}
