package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App{

  val spark = SparkSession.builder()
    .appName("Data Sources and formats")
    .config("spark.master","local")
    .getOrCreate()

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

  /**
    * Reading a DF requires
    *   format - format e.g json
    *   schema or inferSchema=true
    *   one or more options
    *
    */
  val carsDF = spark.read.format("json")
    .schema(carsSchema)
    .option("mode","failFast") // permissive -Default, dropMalformed
    .option("path","src/main/resources/data/cars.json")
    .load()

  println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  carsDF.printSchema()

  carsDF.show

  //cars DF with options map
  val carsDFWithOptionsMap = spark.read.format("json")
    .schema(carsSchema)
    .options( Map(
      "mode" -> "failFast"
      , "path" -> "src/main/resources/data/cars.json"
    ))
    .load()
  carsDFWithOptionsMap.show

  //writing dataframes

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

  spark.createDataFrame(cars).write.format("json").mode(SaveMode.Overwrite).save("src/main/resources/data/cars_duplicate.json")

}
