package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source
import org.apache.spark.sql.functions._


object B_RDDsExercise extends App{

  private val movies_json_file = "D:/shirish/workspace/spark/spark-essentials/src/main/resources/data/movies.json"
  case class Movie(title:String, genre:String, rating:String)


// Exercise 1 :Read movies json as rdd of case class .
  // //todo-ss The solution  is a bit of a hack , where the string values are inside "" -> need to solve this later

  //solution 1 :Without using DF
  val spark = SparkSession.builder()
    .appName("RDD Exercises -1 ")
    .config("spark.master","local")
    .getOrCreate()

  import spark.implicits._ // to call toDF

  val moviesFileLines = Source.fromFile(movies_json_file)
    .getLines()
    .toList

  val moviesTuples =
    moviesFileLines.map(ujson.read(_)).map(
      e => (
        Option(e("Title")).getOrElse("").toString
        ,Option(e("Major_Genre")).getOrElse(" "),
        Option(e("IMDB_Rating")).getOrElse("0")
      )
    )
  ///moviesTuples.foreach(x => println(x._1 + x._2 + x._3 ))
  val movies=  moviesTuples.map(t => Movie(t._1, t._2.toString, t._3.toString) )

  val sc = spark.sparkContext
  val moviesRDD: RDD[Movie] = sc.parallelize(movies)
  moviesRDD.toDF().show()

  //exercise 1:Solution 2 using DF
  //{"Title":"First Love, Last Rites","US_Gross":10876,"Worldwide_Gross":10876,"US_DVD_Sales":null,"Production_Budget":300000,"Release_Date":"7-Aug-98","MPAA_Rating":"R","Running_Time_min":null,"Distributor":"Strand","Source":null,
  // "Major_Genre":"Drama","Creative_Type":null,"Director":null,"Rotten_Tomatoes_Rating":null,"IMDB_Rating":6.9,"IMDB_Votes":207}

  val moviesDF = spark.read
    .option("spark.master","local")
    .json(movies_json_file)

  val moviesWithoutNullsDF = moviesDF.select(column("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where( col("Major_Genre").isNotNull  and col("IMDB_Rating").isNotNull )

  moviesWithoutNullsDF.show()
  // Exercise2  :Show distinct genre as RDD

 // implicit val stockOrdering : Ordering[Movie] = Ordering.fromLessThan( (a, b) => a.genre.compareTo(b.genre) ==0 )

  ///val genreRDD = moviesRDD.map(_.genre).distinct()
  ///genreRDD.toDF().show()

  //exercise 3 : Select all the movies in the drama Genre with IMDB rating greater than 6
  val dramasWithGoodRatingRDD = moviesRDD.filter(
    movie => movie.genre == """"Drama""""
    &&  movie.rating != "null"
    && movie.rating.toDouble > 6.0
  )
 // dramasWithGoodRatingRDD.toDF().show()

  //exercise 4:Show the average rating of movies by genre
  case class GenreAvgRating(genre:String , avgRating: Double)

  moviesRDD.filter( movie => movie.rating != "null")

  val avgRatingBygenreRDD
  = moviesRDD
    .filter( movie => movie.rating != "null")
    .groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating.toDouble).sum / movies.size)
  }
 /// avgRatingBygenreRDD.toDF().show()

  //  .map( (x,y) => x._2.foldLeft(("", 0)("",(acc,ele)=>{ ( 1)  })  ))

}
