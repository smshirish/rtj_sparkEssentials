package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App{

  //create spark session
  val spark = SparkSession.builder()
            .appName("Dataframe Basics")
    .config("spark.master","local")
            .getOrCreate()

  //read data frame

  val firstDF = spark.read.format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  firstDF.show
  firstDF.printSchema()

  // get rows
  firstDF.take(15).foreach(println)

  //spark datatypes use Case Objects to represent the type

  val firstDFSchema = firstDF.schema
  println(firstDFSchema)

  val longType = LongType
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

  //read a DF with your own schema

  val carsDFWithOwnSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  //create row
  val myRow =  Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  //create tuples from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  //create DF by hand
  val manualCarsDF = spark.createDataFrame(cars)  //schema auto inferred
  //note :DFs have schemas , Rows do not

  //create DFs with imlicits , using the implicits from the spark object here.
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "Mpeg", "Cylinders","Displacement","HP", "Weight","Acceleration ","Year of MFG","Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /**
    * Exercise 1: Create a manual DF describing smart phone
    */

  val smartPhones = Seq(
    //model name, Manufacturer , YearOfRelease, OS, cameraPixelMB, RAM , StorageGB
    ("Pixel 6 -Pro","Google",2022,"Android",32,16,256),
    ("Pixel 6 ","Google",2022,"Android",16,8,128)
  )

  val manualDFSmartPhonesWithInferredSchema = spark.createDataFrame(smartPhones)
  manualDFSmartPhonesWithInferredSchema.printSchema()
  manualDFSmartPhonesWithInferredSchema.show

  val manualDFSmartPhonesWithSchema = smartPhones.toDF("model name", "Manufacturer" , "YearOfRelease", "OS", "cameraPixelMB", "RAM" , "StorageGB")
  manualDFSmartPhonesWithSchema.printSchema()
  manualDFSmartPhonesWithSchema.show()


  /**
    * Exercise 2: Read movies.json  file from data folder
    * print schema
    * count number of rows
    */

  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(moviesDF.count())
}
