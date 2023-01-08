package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

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
    StructField("Year", DateType),
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

  //create moviesTuples from moviesTuples
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

  /**
    * Spark read/write options for various formats
    */
  spark.read
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd") // couple it with schema , without schema it cant know which is date type.If spark fails to parse date,it puts null
    .option("allowSingleQuotes","true")
    .option("compression","true") // bzip2, gzip, lz4, snappy, deflate
    .format("json")
    .load("src/main/resources/data/cars.json")  //the format and load calls can be combined to call .json("path method")

  //csv options  symbol,date,price
  val stockSchema = StructType(Array(
    StructField("symbol",StringType)
    ,StructField("date",DateType)
    ,StructField("price",DoubleType)
  ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    //.format("csv")
    .csv("src/main/resources/data/stocks.csv")  // this replaces .format("csv").load(path)

  //parquet format
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")  //parquet is default format, no need to provide format("parquet")

  //text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt")

  //reading from remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise :  Read movies dataframe and then write it as
    * -tab seperated values file
    * -snappy parquet
    * - table public.movies in the postgres DB
    */

   val moviesDF = spark.read
     .option("inferSchema","true")
     .json("src/main/resources/data/movies.json")

  println("!!!!!")
  moviesDF.show


   moviesDF.write
     .mode(SaveMode.Overwrite)
     .option("header","true")
     .option("sep","\t")
     .csv("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
   // .option("compression","snappy") not needed as its default
    .parquet("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
    .mode(SaveMode.Overwrite)
    .save()

  val moviesDFFromDB = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
    .load()
  println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  moviesDFFromDB.show()


}
