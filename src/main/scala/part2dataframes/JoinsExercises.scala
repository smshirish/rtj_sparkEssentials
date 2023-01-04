package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col,column,expr}
object JoinsExercises extends App{

  /**
    * Joins exercises
    * 1: Show all employeed and their max salary (Salary might have changed over time)
    * 2: Show all employees who were never managers
    * 3:find job titles of best paid 10 employees in the company
    */

  //app to run my code snippets
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("JoinsExercises")
    .getOrCreate()

  def readTable2(spark2:SparkSession, tableName: String) = {
    spark2.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.$tableName" )
      .load()
  }

   def readTable(tableName: String) = {
     readTable2(spark,tableName)
  }

  //reading from remote DB
  val employeesDF = readTable("employees")

  //employeesDF.show(false)

  val salariesDF = readTable("salaries").withColumnRenamed("emp_no","emp_no_salaries")




  val maxSalariesDF =  salariesDF.groupBy(col("emp_no_salaries"))
    .agg(
      max(col("salary")).as("max_salary")
    ).withColumnRenamed("emp_no_salaries","emp_no_2")

  ///maxSalariesDF.show

  //exercise 1:Show employees with max salaries
  val empWithMaxSalariesDF = employeesDF
    .join(maxSalariesDF, (employeesDF.col("emp_no") === maxSalariesDF.col("emp_no_2")),"inner" )
    .select("emp_no","first_name","last_name","max_salary").orderBy()
  /// empWithMaxSalariesDF.show(false)

  val managersDF = readTable("dept_manager")

  ///managersDF.show

  //exercise 2:Show employees who were never managers.
  val empsWhoWereNotManagers = employeesDF.join(managersDF, employeesDF.col("emp_no") === managersDF.col("emp_no"),"leftanti" )
  println("!!!!!empsWHoWereNotManagers!!!!!")
  ///empsWHoWereNotManagers.show(false)

  salariesDF.printSchema()
  salariesDF.show(100,false)

  //job title of best paid top 10 employees.
  val latestEmpJobTitlesDF = readTable("titles")
    .groupBy(col("emp_no"))
    .agg( max(col("to_date")).as("latest_title_to_date"))
    .withColumnRenamed("emp_no","emp_no_2")
  ///latestEmpJobTitlesDF.show(100,false)

  val empWithLatestJobTitles = employeesDF.join(latestEmpJobTitlesDF, employeesDF.col("emp_no") === latestEmpJobTitlesDF.col("emp_no_2") )
  empWithLatestJobTitles.show(100,false)

  val latestSalariesDF = salariesDF
    .groupBy(col("emp_no_salaries"))
    .agg( max(col("to_date").desc).as("latest_salary_to_date")).limit(10)

  val empLatestSaleriesWithTItles = empWithLatestJobTitles.join(salariesDF,
    (empWithLatestJobTitles.col("emp_no") === salariesDF.col("emp_no_salaries")) and (empWithLatestJobTitles.col("latest_title_to_date") === salariesDF.col("to_date")))
    .orderBy("emp_no")
  empLatestSaleriesWithTItles.show(100,false)

}
