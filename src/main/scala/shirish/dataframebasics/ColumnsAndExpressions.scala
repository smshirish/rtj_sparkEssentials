package shirish.dataframebasics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  val carsDF = spark.read.format("json").option("inferSchema", "true").load(PATH + "cars.json")
 /// carsDF.printSchema()

  /** Columns
   *  */
  val nameCol = carsDF.col("Name")

  /** Select = Projection */
  val carNamesDF = carsDF.select(nameCol)
  ///carNamesDF.show()

  import spark.implicits._
   val carsDFWithCols: DataFrame = carsDF.select(
    carsDF.col("Name"),
    col("Year"),
     column("Horsepower"),
     'Year, //Scala symbol, converted to COlumn
     $"Horsepower" ,//fancier interpolated string , returns a column object
     expr("Origin") //EXPRESSION
   )
  carsDFWithCols
    ///.show()

  /**
   * You cant mix column nams and expression in single select
    */
  carsDF.select(("Name"),("Origin"))
  //not allowed   carsDF.select(("Name"),col("Origin"))

  /**
   * EXPRESSIONS
   * root
   * |-- Acceleration: double (nullable = true)
   * |-- Cylinders: long (nullable = true)
   * |-- Displacement: double (nullable = true)
   * |-- Horsepower: long (nullable = true)
   * |-- Miles_per_Gallon: double (nullable = true)
   * |-- Name: string (nullable = true)
   * |-- Origin: string (nullable = true)
   * |-- Weight_in_lbs: long (nullable = true)
   * |-- Year: string (nullable = true)
   */
  //simple expr
  val simpleExpr = carsDF.col("Weight_in_lbs")
  val weightInKGExpression = carsDF.col("Weight_in_lbs") / 2.2
  carsDF.select(col("Name"),col("Origin"),col("Weight_in_lbs")
    ,weightInKGExpression.as("weightInKG"),
      expr("Weight_in_lbs / 2.2").as("weightInKG_2")
  )
   /// .show()

  // selectExpr if you have manyexpressions in elect
  val carsWithSelectExprDF = carsDF.selectExpr(
    "Name","Origin","Weight_in_lbs / 2.2"
  )
    ///.show()

  /**
   * DF processing
   */
  //adding a column
  carsDF.withColumn("Weight_in_kg3", col("Weight_in_lbs")/2.2)
    ///.show()
  //rename column
  val carswithColumnRenamedDF =  carsDF.withColumnRenamed("Weight_in_lbs","Weight in pound"  )
  ///carswithColumnRenamedDF.show()
  // Careful with selectExpr and column names (e.g. spaces etc will break expr parser
  carswithColumnRenamedDF.selectExpr("`Weight in pound`") //excape with `
  //remove a column
  carswithColumnRenamedDF.drop("Name")
  ///  .show

  /**
   * Filtering DF
   */
  //filtering in 2 ways , with filter and where
  val euCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val euCarsDF2 = carsDF.where(col("Origin") =!= "USA")
     /// .show
  // Filtering with expression strings
  val usCarsDF = carsDF.filter("Origin = 'USA' " )
  val usCarsDF2 = carsDF.where("Origin = 'USA' ")
   /// .show()
  //chain filters
  val usPowerfulCarsDF = carsDF.filter("Origin = 'USA' " ).filter(" HorsePower > 150")
  val usPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" ).filter(col("HorsePower")  > 150)
  // .show()
  val usPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' AND HorsePower > 150")
  //.show()
  val usPowerfulCarsDF4 = carsDF.filter(col("Origin") === "USA" and col("HorsePower")  > 150)
  //  .show()

  /**
   * Union
   */
  val more_carsDF = spark.read.format("json").option("inferSchema", "true").load(PATH + "more_cars.json")
  val allCarsDF = carsDF.union(more_carsDF)
  ///println(s"Cars in carsDF are ${carsDF.count()} and all carrsDF has ${allCarsDF.count()} ")

  //Distinct
  carsDF.select("Origin").distinct()
    .show()

}
