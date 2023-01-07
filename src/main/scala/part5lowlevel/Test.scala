package part5lowlevel

import org.apache.spark.sql.SparkSession

import scala.io.Source

object Test extends App{


  val spark = SparkSession.builder()
    .appName("RDD Exercises -1 ")
    .config("spark.master","local")
    .getOrCreate()

  val lines = Source.fromFile("D:/shirish/workspace/spark/spark-essentials/src/main/resources/data/movies.json")
    .getLines()
    .toList

  val tuples =
    lines.map(ujson.read(_)).map(
      e => (
        Option(e("Title")).getOrElse("").toString
        , Option(e("Major_Genre")).getOrElse(""),
        Option(e("IMDB_Rating")).getOrElse(0)
      ) //(e("Major_Genre").isNull "" else e("Major_Genre").str)
    )

  case class Movies(title:String , genre:String, rating:String)

   val movies=  tuples.map( t => Movies(t._1, t._2.toString, t._3.toString) )
  movies.foreach(println(_))
}
