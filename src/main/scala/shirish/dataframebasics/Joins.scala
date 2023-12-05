package shirish.dataframebasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Joins extends App {

  val PATH = "src/main/resources/data/"

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("DFBasics")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json(PATH + "guitars.json")
  val guitaristsDF = spark.read.option("inferSchema", "true").json(PATH + "guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json(PATH + "bands.json")

  private val joinCOndition =  guitaristsDF.col("band") === bandsDF.col("id")
  //join

  val guitaristBandsDF = guitaristsDF.join(bandsDF, joinCOndition,"inner")
  guitaristsDF.join(bandsDF, joinCOndition,"left_outer")
   /// .show() // inner join rowws  + all rows in left table

  guitaristsDF.join(bandsDF, joinCOndition, "right_outer")
  ///.show() // inner join rowws  + all rows in right table

  guitaristsDF.join(bandsDF, joinCOndition, "outer") // all rows from both tables
   /// .show

  /// Semi joins, same as inner join but cut out data from other table(everything in left DF for which there is a row sastsfying join condition in right DF)
  guitaristsDF.join(bandsDF, joinCOndition, "left_semi")
   ///.show()

  //anti-join , show only those rows which have no match in right table ((everything in left DF for which there is no row sastsfying join condition in right DF))
  guitaristsDF.join(bandsDF, joinCOndition, "left_anti")
  ///.show()

  /**
   * Joins :points to rememebr
   * Handle duliocate column name as one of below
   * 1:rename
   * 2:Drop one of duplicate
   */

  //following will crash spark as the ID column is ambigious ..tehre are 2 of them
  //error:Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.;
  //guitaristBandsDF.select("id","band")
  //sol 1: rename
  guitaristsDF.join(bandsDF.withColumnRenamed("id","band"), "band")//band column in both DF and used to join,, result has only one band column ,no issue
    .select("id","band")
    ///.show()

  //sol2:Drop the duplicate column
  guitaristBandsDF.drop(bandsDF.col("id")) //must use qualified name from original DF to adccess the column in result DF
    .select("id", "band")
  ///.show()

  //sol3:modify the offending col and keep it
  val modBandsDF = bandsDF.withColumnRenamed("id","bandID")
  guitaristsDF.join(modBandsDF, guitaristsDF.col("id") === modBandsDF.col("bandID"))
    .select("id","bandID")
    .show

  //Using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarID"), expr("array_contains(guitars, guitarID)"))
    .show()


}