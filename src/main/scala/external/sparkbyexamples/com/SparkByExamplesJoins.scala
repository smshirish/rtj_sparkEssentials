package external.sparkbyexamples.com

import org.apache.spark.sql.SparkSession

object SparkByExamplesJoins extends App {

  //based on https://sparkbyexamples.com/spark/spark-sql-dataframe-join/

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("SparkByExamplesJoins")
    .getOrCreate()

  val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
    "emp_dept_id","gender","salary")
  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

  val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )

  val deptColumns = Seq("dept_name","dept_id")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)

  //default inner join , only rows with match in both data frames returned
  //empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id") ).show(false)

  //ful outer, all rows from both DF returned
  //empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter" ).show(false)

  //left or left outer. All rows from lest dataset (EMP) are returned and non matching rows from right are dropped (i.e. dept 30 is dropped , while emp 6 without dept shown)
  empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftouter" ).show(false)

  //semi joins are efficient select on inner join. i.e. The selected result works like inner join , but the columns selected are decided by left or right
  // left semi join selects all columns from left dataframe, for all records of left data set which has a match in right data set
  //join below shows all cilumns of left DF (employee ) , which have a match Dept . emp 6 without match in dept is dropped ,selection of rows is just like inner join
  empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi" ).show(false)

  //join below shows all cilumns of right DF (Dept ) , where EMP( have a match Dept . emp 6 without match in dept is dropped ,selection of rows is just like inner join
  empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"semi" ).show(false)
  //the left and right not decided by condition but by where the join is called.above and below (commented , )Both results are same
  //empDF.join(deptDF, deptDF("dept_id") === empDF("emp_dept_id"),"semi"   ).show(false)

  //val empWithoutValidDept = empDF.join(deptDF, empDF.col("emp_dept_id") ===  deptDF.col("dept_id") )


  //left anti shows rows from left DF which have no matching records
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti")
    .show(false)


}
