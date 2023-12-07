package shirish.sparktypesanddatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
object CommonTypes extends App {
  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json(PATH + "movies.json")
  ///moviesDF.show()

  //Add a plain value to Spark DF
  moviesDF.select( col("TItle"), lit(17).as("plain_text"))
 ///   .show

  private val dramaFilter: Column = col("Major_Genre") === "Drama"
  private val goodRatingFilter: Column = col("IMDB_Rating") > 7.0
  private val preferredFilter: Column = dramaFilter and goodRatingFilter

  //multiple ways of filtering
  moviesDF.select("Title").where(dramaFilter)
   /// .show

  moviesDF.select("*").where(dramaFilter)
   /// .show

  moviesDF.select(col("Title"), preferredFilter).where( preferredFilter)
    //.show

  //you can show filters as columns
  val moviesWithGoodNessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  //filter on boolean
  moviesWithGoodNessFlagDF.where("good_movie") // where(col("good_movie") === "true")
///  .show

  //negations
  moviesWithGoodNessFlagDF.where(not(col("good_movie")))
  //  .show()

  // Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
 /// println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)

  /**
    String Operations
   */

  val carsDF = spark.read.option("inferSchema","true").json(PATH + "cars.json")
  // capitalization: initcap, lower, upper
  carsDF.select(col("Name"), initcap(col("Name")))
///    .show()


  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))
  ///  .show()

  carsDF.select("*").where(col("Name").contains("vw"))
  ///  .show()

  //Regex
  val regExString = "volkswagen|vw"
  carsDF.select(col("Name"),
    regexp_extract(col("Name"), regExString,0).as("reg_ex_name"))
    .orderBy(col("reg_ex_name").desc_nulls_last)
   /// .show  //show some not null for regex result

  //drop unwanted column
  val vwDF = carsDF.select(col("Name"),
      regexp_extract(col("Name"), regExString, 0).as("regex_extract"))
  .where(col("regex_extract") =!= "")
    .drop("regex_extract")
  /// .show  //show some not null for regex result

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regExString, "People's Car").as("regex_replace")
  )
    .show()
  //drop unwanted column
  private val regExNameCol: Column = regexp_extract(col("Name"), regExString, 0)
  carsDF.select(col("Name"),
      regExNameCol.as("reg_ex_name"))
    .drop("reg_ex_name")
  ///  .show //show some not null for regex result

  carsDF.select("*").where(regExNameCol=!= "")
  ///  .show


  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */

  def getCarNames(): List[String] = { List("volkswagen","Ford", "Mercedes-Benz")}

  //solution v1:With regex
  val complexRegExString = getCarNames().map( _.toLowerCase).mkString("|")
  private val complexRegExNameCol: Column = regexp_extract(col("Name"), complexRegExString, 0)

  val exerciseSolutionDF = carsDF.select(col("Name"),
      complexRegExNameCol.as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  exerciseSolutionDF
    ///.show()

  //solution v2:With contains
  // col("Name").contains(nameString)
  val carNameContainsFilters = getCarNames().map(_.toLowerCase).map( name => col("Name").contains(name))

  val bigFilter = carNameContainsFilters.fold( lit(false)) ( (combinedFilter , newFilter) => (combinedFilter or newFilter))
  carsDF.where(bigFilter)
    .show()



}
