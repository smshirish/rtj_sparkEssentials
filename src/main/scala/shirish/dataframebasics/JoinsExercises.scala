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
  ///employeesDF.show()
  val salariesDF = readDBTable("public.salaries")
 /// salariesDF.show()
  val maxSalariesDF = salariesDF.groupBy(col("emp_no")).max("salary")
  ///maxSalariesDF.show()

  val employeeSalariesDF = employeesDF.join(maxSalariesDF, employeesDF.col("emp_no") === maxSalariesDF.col("emp_no"))
  // employeeSalariesDF.show()

  // 2: show all emp who were never managers

  val deptMAnagersDF = readDBTable("public.dept_manager")
 // deptMAnagersDF.show()

  deptMAnagersDF.filter(col("emp_no") === "12940")
    ///.show()
  val empWhoWereNeverManagersDF = employeesDF.join(deptMAnagersDF, employeesDF.col("emp_no") === deptMAnagersDF.col("emp_no"),"left_anti")
//  empWhoWereNeverManagersDF.show()

  //3:Job titles of best paid 10 employees in the company
  import spark.implicits._
  val titlesDF = readDBTable("public.titles")

  ///val latestJobTitleDF = titlesDF.groupBy(col("emp_no")).agg(max("from_date"))
  //latestJobTitleDF.show()
 // employeesDF.orderBy(col("emp_no").desc_nulls_last).show()
  println("XXXXXXX ")
  salariesDF.orderBy(col("emp_no").desc_nulls_last)
    .show()

  val latestEmploymentDF = salariesDF.groupBy(col("emp_no")).agg(max("from_date").as("from_date")).orderBy(col("emp_no").desc_nulls_last)
  println("YYYYYY ")
  latestEmploymentDF.show()

  val latestSalaryDF = salariesDF.join(latestEmploymentDF, ( (salariesDF.col("emp_no") === latestEmploymentDF.col("emp_no"))
    and (salariesDF.col("from_date") === latestEmploymentDF.col("from_date"))))
  println("ZZZ")
 /// latestSalaryDF.filter(salariesDF.col("emp_no")  === "499990").show()
  latestSalaryDF.orderBy(col("salary").desc_nulls_last).show()
  println("11111")

  println("2222")
  titlesDF.show()  //emp_no|           title| from_date|

  println("3333")
  latestSalaryDF.dtypes.foreach(f=>println(f._1+","+f._2))
  println("44444")
  latestSalaryDF.join(titlesDF, (latestSalaryDF.col("emp_no") === titlesDF.col("emp_no"))).show


}
