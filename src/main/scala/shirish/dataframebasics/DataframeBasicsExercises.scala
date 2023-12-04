package shirish.dataframebasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object DataframeBasicsExercises extends App {

  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  val firstDF = spark.read.format("json").option("inferSchema", "true").load(PATH + "cars.json")

  /**
   * Create manual DF for Smart phone
   * -make
   * model
   * screen dimension
   * camera megapixel
   *
   */

  val schema = StructType(
    Array(StructField("make",StringType),
      StructField("model",StringType),
      StructField("screen-dimension-inches",DoubleType),
      StructField("camera-megapixel",DoubleType))
  )

  val seq = Seq(("Samsung","Galaxy 16",17,200),
    ("Iphone","Iphone 16",17,2500),
      ("Nokia","Nokia xx",25,1000))

  val mobileDF = spark.createDataFrame(seq)

  import spark.implicits._

  val mobilephonedF2 = seq.toDF("make","model","screen-size","PPixel")
  mobilephonedF2.printSchema()

  /**
   * Read second file, movies,json as dataframe
   * Print schema and count the rows
   */
  val moviesDF = spark.read.format("json").option("inferSchema", "true").load(PATH + "movies.json")
 // moviesDF.printSchema()
 // moviesDF.count()
  println(s"The movies DF has ${moviesDF.count()} rows")


}
