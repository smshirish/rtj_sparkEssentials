package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,column,expr}


object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("Columns And expressions")
    .config("spark.master","local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  //columns selection (projection)
  val firstCOlumn = carsDF.col("Name")
  val carNamesDF = carsDF.select(firstCOlumn)

  carNamesDF.show()

  //various select methods
  import spark.implicits._
  val projectedDF= carsDF.select(
    carsDF.col("name")
    ,col("Acceleration")
    ,column("Weight_in_lbs")
    ,'year //scala symbol, autoconverted to column
    ,$"Horsepower" //fancier interpolated string, returns a  column
    ,expr("Origin")//expression
  )

  projectedDF.show

  //alternatively pass all column names as string (This format not possible to mix with any of above slection methods)
  carsDF.select("name", "Origin")

  //expressions -> result in transformations (narrow or wide)  , column is an expression below
  //simplest expression
  val simplestExpression = carsDF.col("Weight_in_lbs") //narrows transformation , each inout partition in original data frame has one output partition in the new data frame
  val weightInKGExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    col("name")
    ,col("Weight_in_lbs")
    ,weightInKGExpression.as("WeightInkg")
    ,expr("Weight_in_lbs / 2.2 ").as("WeightInkg_expr")
  )

  //selectExpr is a shorthand for  select( expr()) syntax

  val carsWithWeightDFSelectExpr = carsDF.selectExpr(
    "name"
    ,"Weight_in_lbs"
    ,"Weight_in_lbs / 2.2 "
  )
  carsWithWeightDFSelectExpr.show

  //dataframe processing
  //adding a column
  val carsDF_weightsInKg3 = carsDF.withColumn("weightsInKg3",col("Weight_in_lbs") / 2.2 )
  carsDF_weightsInKg3.show

  val carsDFWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  carsDFWithColumnRenamed.show

  // expressions need backtik with using spaces
  carsDFWithColumnRenamed.selectExpr("`Weight in pounds`")

  carsDF.drop("Year")

  //filtering
  //Note the  the equality is checked with =!= and not with !=
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  europeanCarsDF.show
  //filter and where are exactly same
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  //filtering with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  americanCarsDF.show

  //chain filters
  val americanCarsWithHighPowerDF = carsDF.filter(col("Origin") === "USA").filter(col("HorsePower") > 150)
    americanCarsWithHighPowerDF.show()

  //chaining filters nice way with and
  val americanCarsWithHighPowerDF2 = carsDF.filter(
    (col("Origin") === "USA")
      .and((col("HorsePower") > 150))
  )
  americanCarsWithHighPowerDF2.show()

  //even better AND -> More natural without braces
  val americanCarsWithHighPowerDF3 = carsDF.filter(
    (col("Origin") === "USA")
      and (col("HorsePower") > 150)
  )
  americanCarsWithHighPowerDF3.show()

  //filters with expressions
  val americanCarsWithHighPowerDF_expressions = carsDF.filter("Origin = 'USA' AND HorsePower > 150 ")
  americanCarsWithHighPowerDF_expressions.show

  //union -> Adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) //both DF have same schema
  allCarsDF.show

  val allCOuntriesDF = allCarsDF.select("Origin").distinct()
  allCOuntriesDF.show












}
