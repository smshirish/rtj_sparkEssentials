package part3typesdatasets

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object CommonTypes extends App{

  val spark = SparkSession.builder()
    .appName("TypesDatasets")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

 /// moviesDF.printSchema()
 /// moviesDF.show(100,true)

  //adding a plain value to the DF
  val plainValueDF: DataFrame = moviesDF.select(col("Title"), lit("47").as("PlainValue"))
  ///plainValueDF.show()

  //Booleans
  val dramaFilter: Column = moviesDF.col("Major_Genre") equalTo  "Drama"
  val goodRatingFilter = moviesDF.col("IMDB_Rating") > 7.0
  private val preferredFilter =  dramaFilter and goodRatingFilter
  moviesDF.where(preferredFilter).show()

  //select the filter as a value of the DF
  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("Good_Movies")).orderBy("Good_Movies")
  //moviesWithGoodnessFlagDF.show(3000,true)

  //boolean selection with boolean  comparison -ShortCut
  //moviesWithGoodnessFlagDF.where("Good_Movies").show(3000,true)  //this is same as where(col("Good_Movies") === "true")
  //negations
  moviesWithGoodnessFlagDF.where(not(col("Good_Movies"))).show(3000,true)

  /**
  Number types
    */

  //math operators
  val moviesWithAVGRatingDF = moviesDF.select(col("Title"), (( col("Rotten_Tomatoes_Rating")/10 + col("Imdb_Rating"))/2).as("AvgRating"))
  //moviesWithAVGRatingDF.show(300,true)

  //correlations
  //println(moviesDF.stat.corr("Rotten_Tomatoes_Rating","Imdb_Rating") )// Corr is an Action and not transformation (triggers computation immediately)

  /**
    * Strings
    */
   val carsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/cars.json")

  //capitaisation initcap,upper,lower
  carsDF.select( initcap(col("Name")))
    ///.show()

  //contains
  carsDF.select(col("*")).where(col("Name").contains("volkswagen"))
   /// .show(100,false)

  //regex
  val regExString = "volkswagen|vw"
  val vwCarsDF = carsDF.select(col("Name"), regexp_extract(col("Name"), regExString, 0).as("regex_extract"))
    .where( col("regex_extract") =!= "").drop("regex_extract")
  ///vwCarsDF.show(300,false)

  //regex replase
  vwCarsDF.select(col("Name"), regexp_replace(col("Name"), regExString, "Peoples car"))
   /// .show(300,false)

  /**
    * Exercise: Filter the cars DF by the list of car names obtained by an API call
    * versions:
    * - Contains
    * - Regex
    */

  val carNames : List[String] = List("Volkswagen", "Toyota","Ford")
    //version 1- regex verison -easy one
  val compexRegEx = carNames.map( _.toLowerCase()).mkString("|")

  val selectedCarsDF = carsDF.select(col("Name"), regexp_extract(col("Name"), compexRegEx, 0).as("regex_extract"))
    .where( col("regex_extract") =!= "").drop("regex_extract")
  ///selectedCarsDF.show(300,false)

  //version 2-Contains version

  val carNameFilters = carNames.map(_.toLowerCase()).map( name => col("Name").contains(name) )

  val bigFilter = carNameFilters.fold(lit(false)) ( (combinedFilter, newCarNameFilter )=> combinedFilter or newCarNameFilter)
  println(bigFilter)

  carsDF.select(col("*")).where(bigFilter)
    .show(300,false)

}

