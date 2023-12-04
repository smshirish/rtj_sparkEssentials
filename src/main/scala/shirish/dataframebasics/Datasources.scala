package shirish.dataframebasics

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Datasources extends App {

  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Data sources")
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
   * Reading a DF need
   * -format
   * -schema or inferSchema=true
   * -path
   * -zero or more options
   */
  val carsDF = spark.read.format("json")
    .option("inferSchema","true")
    .option("mode","failFast") //permissive (default),dropMalformed
    .load(PATH + "cars.json")

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map("inferSchema" -> "true",
      "mode" -> "failFast"
    ,"path"-> "src/main/resources/data/cars.json"))
    .load

  /// Writing dataframe
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save(PATH + "cars_dupe.json")

  //Json flags
  val carsDFJSON = spark.read
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd") // couple with Schema, if parsing fails, spark will put nulls.
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed")
    .json(PATH + "cars.json")

  //csv flags
  val stocksSchema = StructType(
    Array(StructField("symbol",StringType), //symbol,date,price
      StructField("date",DateType),
      StructField("price",DoubleType)))

  spark.read
    .schema(stocksSchema)
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .csv(PATH + "stocks.csv")

  //parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save(PATH + "carsDFParquet")

  val employeesDF = spark.read
    .format("jdbc")
    .option("user","docker")
    .option("password","docker")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("dbtable","public.employees")
    .load()

  ///employeesDF.show()

  /**
   * Exercises
   * Read movies DF and then write it as
   * 1:Tab seperated file
   * 2:snappy parquet
   * 3:table public.movies in postgres DB
   *
   */

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json(PATH + "movies.json")

  moviesDF.show
  println("moviesDF ???????????????????????????????????????")
  moviesDF.write.mode(SaveMode.Overwrite).save(PATH+ "movies_parquet")
  moviesDF.write
    .option("header","true")
    .option("sep","\t")
    .mode(SaveMode.Overwrite)
    .save(PATH + "movies_tab_seperated")

  val moviesDF_2 = spark.read
    .option("inferSchema", "true")
    .parquet(PATH + "movies_parquet")

  moviesDF_2.show()
  println("moviesDF_2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

  /**
   * val employeesDF = spark.read
   * .format("jdbc")
   * .option("user","docker")
   * .option("password","docker")
   * .option("driver","org.postgresql.Driver")
   * .option("url","jdbc:postgresql://localhost:5432/rtjvm")
   * .option("dbtable","public.employees")
   * .load()
   */
  moviesDF.write
  .format("jdbc")
   .option("user","docker")
   .option("password","docker")
   .option("driver","org.postgresql.Driver")
   .option("url","jdbc:postgresql://localhost:5432/rtjvm")
   .option("dbtable","public.movies")
    .save()

  val moviesDFFromDB = spark.read
    .format("jdbc")
    .option("user", "docker")
    .option("password", "docker")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("dbtable", "public.movies")
    .load()

  moviesDFFromDB.show()


}
