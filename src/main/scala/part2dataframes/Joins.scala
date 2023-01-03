package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Joins")
    .getOrCreate()

  val bandsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/bands.json")
  val guitaristsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers.json")
  val guitarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/guitars.json")


  ///bandsDF.show()
  ///guitaristsDF.show
  ///guitarsDF.show

  //first join, using innerJoin (default join type)
  private val guitaristBandJoinConCondition = bandsDF.col("id") === guitaristsDF.col("band")
  val guitaristBandsDF = guitaristsDF.join(bandsDF, guitaristBandJoinConCondition ,"inner" )

  ///guitaristBandsDF.show

  //left outer join == Everything in inner join plus rows in left table
  val guitaristBandsleftOuterDF = guitaristsDF.join(bandsDF, guitaristBandJoinConCondition,"left_outer")
  //guitaristBandsleftOuterDF.show

  //right outer join == Everything in inner join plus rows in right table
  val guitaristBandsrightOuterDF = guitaristsDF.join(bandsDF, guitaristBandJoinConCondition,"right_outer")
  ///guitaristBandsrightOuterDF.show

  //full outer join == Everything in inner join plus rows in both tables
  val guitaristBandsFullOuterDF = guitaristsDF.join(bandsDF, guitaristBandJoinConCondition,"outer")
  ///guitaristBandsFullOuterDF.show

  ///semi joins, show data only from one table and relatd data in other table
  val guitaristBandssemiJoinDF = guitaristsDF.join(bandsDF, guitaristBandJoinConCondition,"left_semi")
  ///guitaristBandssemiJoinDF.show

  //Anti join , record which has no match
  //guitaristsDF.join(bandsDF, guitaristBandJoinConCondition,"left_anti")

  //things to watch in joins
  //duplciate column name id in selected row. following will crash

  //guitaristBandsDF.select("id","band").show()
  // " org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.

  //option 1, rename join column to show bands only once
  ///guitaristBandsDF.show
  ///guitaristsDF.join(bandsDF.withColumnRenamed("id","band"),"band" ).show

  //option 2 , drop the offending column
  ///guitaristBandsDF.drop(bandsDF.col("id")).show

  //option 3, rename the offending column
  val bandsMdDF = bandsDF.withColumnRenamed("id","bandID")
  guitaristsDF.join(bandsMdDF, guitaristsDF.col("id") === bandsMdDF.col("bandID") ).show()

  //compley types in joins, join by guitaristID
  import org.apache.spark.sql.functions.{col,column,expr}
  guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarID"), expr("array_contains(guitars, guitarID)") ).show

  /**
    * Joins exercises
    * 1: Show all employeed and their max salary (Salary might have changed over time)
    * 2: Show all employees who were never managers
    * 3:find job titles of best paid 10 employees in the company
    */

}
