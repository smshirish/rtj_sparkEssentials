package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import part1recap.SparkUtils

import java.io.File

object C_3_SparkSqlExercises extends App{

  val sparkDBBasePath = "src/main/resources/warehouse"
  val spark = SparkSession.builder()
    .appName("C_3_SparkSqlExercises")
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
  /// movies2DF.show()

  /**
    * Exercise2: How many employees were hired between jan 1 2000 and jan 1 2001.
    */
  //load and also make the table visible for spark as temp table.
  SparkUtils.transferTables(spark,SparkUtils.SPARK_DB_PATH, List("employees","departments","dept_emp","salaries"),false)
  val employeesHiredBetweenDatesDF = spark.sql(" select * from employees where  hire_date > '1985-01-01' and hire_date <  '1988-01-01' ")
  /// employeesHiredBetweenDatesDF.show()

  /**
    * Exercise2: Show the average salaries for the  employees were hired between those dates, grouped by department .
    */
  val departmentsDF = spark.sql("select * from departments ")
  /// departmentsDF.show()  // |dept_no|         dept_name|

  //val empWithDepartmentsDF =

  val deptEmpDF = spark.sql("select * from dept_emp ")
///  deptEmpDF.show() //|emp_no|dept_no| from_date|   to_date|

  ///val salariesDF = spark.sql("select * from salaries ")
  //salariesDF.show()  // |emp_no|salary| from_date|   to_date|

  val latestSalariesDF = spark.sql(
    """
      |select max(salary) as salary  , emp_no
      | from salaries
      |  group by emp_no
      |""".stripMargin)
  ///latestSalariesDF.show()
  latestSalariesDF.createOrReplaceTempView("latestSalaries")

  val empWithLatestSalAndDeptDF = spark.sql(
    """
      | select s.salary, d.dept_name
      | , e.emp_no , e.first_name ,e.hire_date, e.last_name, e.birth_date, e.hire_date, e.gender
      |
      | from employees e , latestSalaries s , dept_emp de , departments d
      |  where  e.hire_date > '1985-01-01' and e.hire_date <  '1988-01-01'
      |  and e.emp_no = s.emp_no
      |  and e.emp_no = de.emp_no
      |  and de.dept_no = d.dept_no
      |""".stripMargin)
 // empWithLatestSalAndDeptDF.show() //|emp_no|birth_date|first_name|  last_name|gender| hire_date|

  val avgSalaryPerDeptDF = spark.sql(
    """
      | select avg(s.salary) , d.dept_name
      |
      | from employees e , latestSalaries s , dept_emp de , departments d
      |  where  e.hire_date > '1985-01-01' and e.hire_date <  '1988-01-01'
      |  and e.emp_no = s.emp_no
      |  and e.emp_no = de.emp_no
      |  and de.dept_no = d.dept_no
      |  group by d.dept_name
      |""".stripMargin)
  avgSalaryPerDeptDF.show()

}
