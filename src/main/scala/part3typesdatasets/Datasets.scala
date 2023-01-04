package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date


object Datasets extends App{

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Datasets")
    .getOrCreate()


  val numbersDF = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .format("csv")
    .load("src/main/resources/data/numbers.csv")

  ///numbersDF.printSchema()

  //create types dataset from DF.
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS :Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)  //normal scala operations can be applied
   /// .show(300,false)


  //{"Name":"amc ambassador dpl", "Miles_per_Gallon":15, "Cylinders":8, "Displacement":390,
  // "Horsepower":190, "Weight_in_lbs":3850, "Acceleration":8.5, "Year":"1970-01-01", "Origin":"USA"}
  case class Car(
                  Name:String ,
                  Miles_per_Gallon:Option[Double],
                  Cylinders:Long,
                  Displacement:Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs:Long,
                  Acceleration:Double,
                  Year:String,
                  Origin:String
                )

  private def readDF (fileName:String) = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }
  val carsDF = readDF("cars.json")

  /// carsDF.show()
  //below imports not needed is you import spark.implicits...the imports are to illustrate the encoder mechanism
  //add encoder for Car
  //implicit val carEncoder = Encoders.product[Car] //all case classes extend Product type
  //implicit val stringEncoder = Encoders.STRING


  import spark.implicits._
  val carsDS :Dataset[Car] = carsDF.as[Car]
  carsDS.map( car => car.Name.toUpperCase())
    .show()

  /**
    * exercise:
    * How many cars we have
    * How many poserful cars we have (HP > 100)
    * Average horsepower of the dataset
    *
    */

  //how many cars we have
  private val carsCount: Long = carsDS.count()
  println(carsCount)

  //powerful cars
  carsDS.filter( car =>  car.Horsepower.getOrElse(0L) > 140L)
  ///   .show()

  val avgHP =  carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _ )/ carsCount
 /// println(avgHP)

  //DS also has access to all DF functions
  carsDS.select(avg(col("Horsepower")))
 ///   .show()

  /**
  Joins on DS
   */
  //{"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}
  case class Band(id: Long, name: String, hometown: String, year: Long)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Guitar(id: Long, model: String, make: String, guitarType: String)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  //join
  val guitarPlayerBandsDS : Dataset[(GuitarPlayer, Band)]= guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  ///guitarPlayerBandsDS.show()

  /**
    * Exercise : Join guitarsDS and guitasPlayersDS
    */

  private val guitarsGuitarPlayers: Dataset[(GuitarPlayer, Guitar)] = guitarPlayersDS.joinWith( guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
 ///  guitarsGuitarPlayers.show()

  //DS grouping -> Joins and Groups are wide transformation and they will cause shuffle
  carsDS.groupByKey(_.Origin).count()
    .show()


}
