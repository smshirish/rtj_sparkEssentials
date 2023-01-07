package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object B_RDDsExercise extends App{

  //
  // {"Title":"The Land Girls","US_Gross":146083,"Worldwide_Gross":146083,"US_DVD_Sales":null,
  // "Production_Budget":8000000,"Release_Date":"12-Jun-98","MPAA_Rating":"R","Running_Time_min":null
  // ,"Distributor":"Gramercy","Source":null,"Major_Genre":null,"Creative_Type":null,"Director":null,
  // "Rotten_Tomatoes_Rating":null,"IMDB_Rating":6.1,"IMDB_Votes":1071}

  // Exercises :Read movies json as rdd of case class


  val spark = SparkSession.builder()
    .appName("RDD Exercises -1 ")
    .config("spark.master","local")
    .getOrCreate()

  import spark.implicits._ // to call toDF

  val sc = spark.sparkContext

  case class Movies(title:String , genre:String, rating:String)

  def readStocks(fileName:String) =
    Source.fromFile(fileName)
      .getLines()
      .map( line => line.split(",|\\{|\\}|\\:"))
      //.map( line => line.split(",|\\{|\\}|\\:"))
    //  .map(tokens => Movies(tokens(1),tokens(21),tokens(28).toDouble))
      .toList

  //test code to parse the file into tokens  D:/shirish/workspace/spark/spark-essentials/src/main/resources/data/movies_copy.json
  val rdds2: Seq[Array[String]] = readStocks("src/main/resources/data/movies_copy.json")
  //val rdds  = readJson("src/main/resources/data/movies.json")
  rdds2.foreach( _.foreach(println(_)))
  ///println(rdds2)

  //map to Movies
  val moviesSeq: Seq[Movies] = Source.fromFile("D:/shirish/workspace/spark/spark-essentials/src/main/resources/data/movies_copy.json")
    .getLines()
    .map(line => line.split(",|\\{|\\}|\\:"))  // Split each line of json by seperators { } , : -> This gives per line key values as tokens.
    .map(tokens => Movies(tokens(2), tokens(22), tokens(29)))
    .toList


  ///val moviesRDD1: RDD[Movies] = sc.parallelize(moviesSeq)
  ///moviesRDD1.toDF().show()




  lines.map(ujson.read(_)).map(
    e => ( e("Title").str, e("Major_Genre") )
  )



}
