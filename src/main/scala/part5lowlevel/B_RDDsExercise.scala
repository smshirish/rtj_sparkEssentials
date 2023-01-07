package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object B_RDDsExercise extends App{

// Exercises :Read movies json as rdd of case class

  val spark = SparkSession.builder()
    .appName("RDD Exercises -1 ")
    .config("spark.master","local")
    .getOrCreate()

  import spark.implicits._ // to call toDF

  val sc = spark.sparkContext

  case class Movies(title:String , genre:String, rating:String)


  val lines = Source.fromFile("D:/shirish/workspace/spark/spark-essentials/src/main/resources/data/movies.json")
    .getLines()
    .toList

  val tuples =
    lines.map(ujson.read(_)).map(
      e => (
        Option(e("Title")).getOrElse("").toString
        ,Option(e("Major_Genre")).getOrElse(""),
        Option(e("IMDB_Rating")).getOrElse(0)
      )
    )

  val movies=  tuples.map( t => Movies(t._1, t._2.toString, t._3.toString) )

  val moviesRDD: RDD[Movies] = sc.parallelize(movies)
 moviesRDD.toDF().show()

  // Exercise2  :Show distinct genre as RDD

 // implicit val stockOrdering : Ordering[Movies] = Ordering.fromLessThan( (a, b) => a.genre.compareTo(b.genre) ==0 )

  ///val genreRDD = moviesRDD.map(_.genre).distinct()
  ///genreRDD.toDF().show()

  //exercise 3 : Select all the movies in the drama Genre with IMDB rating greater than 6
  val dramasWithGoodRatingRDD = moviesRDD.filter( movie => movie.genre == """"Drama"""" && movie.rating.toDouble > 6.0 )//
  dramasWithGoodRatingRDD.toDF().show()

}
