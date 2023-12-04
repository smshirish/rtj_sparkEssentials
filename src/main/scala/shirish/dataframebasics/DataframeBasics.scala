package shirish.dataframebasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataframeBasics extends App {

  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("DFBasics")
    .getOrCreate()

  val firstDF = spark.read.format("json").option("inferSchema","true").load(PATH + "cars.json")

  firstDF.printSchema()
  //fetch rows from DF
  firstDF.take(10).foreach(println)

  /**
   * A Spark schema structure that describes a small cars DataFrame.
   */
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  //obtain a schema
  val schema = firstDF.schema
  println(schema)

  //read using known schema
  val carsDFWithSchema = spark.read.format("json").schema(carsSchema).load(PATH + "cars.json")
  ///carsDFWithSchema.show()

  /**
   * A "manual" sequence of rows describing cars, fetched from cars.json in the data folder.
   */
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars)
  println("(!!!!!!!!!!!!!!!!!!!!)")
  manualCarsDF.show()

  //DF with implicits
  import spark.implicits._
  val manualCarsDFWithImplists = cars.toDF("Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs"
    , "Acceleration", "Year", "Origin")
  manualCarsDFWithImplists.printSchema()
  manualCarsDF.printSchema()

}
