package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source


object A_RDDs extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("RDDs")
    .getOrCreate()

  import spark.implicits._ // to call toDF
  val sc = spark.sparkContext

  //parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)  //distributed collection of INT

  //2 -reading from file
  case class StockValue(symbol:String , date: String , price:Double)

  def readStocks(fileName:String) =
    Source.fromFile(fileName)
      .getLines()
      .drop(1) //ignore header
      .map( line => line.split(","))
      .map(tokens => StockValue(tokens(0),tokens(1),tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  /// moviesRDD.toDF().show()

  //2 -reading from file B

  //returns an RDD of String
  val stocksRDD2: RDD[StockValue] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  ///stocksRDD2.toDF().show()

  val stocksDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksDS = stocksDF.as[StockValue]

  val stocksRDD3 = stocksDS.rdd
 /// stocksRDD3.toDF().show()  //rdd to DF , you loose type information

  stocksDF.rdd //returns RDD of type Dataset[Row] and not Dataset[StockValue]

  //Rdd to dataset
  val fromRDD_DS = spark.createDataset(stocksRDD3)
///  fromRDD_DS.show()

  ///RDD Transformations
  val microsoftStocks: RDD[StockValue] = stocksRDD.filter(_.symbol == "MSFT") //lazy transform
  ///println(microsoftStocks.count())  //eager Action

  val companyNamesRDD = stocksRDD.map( _.symbol).distinct() //also lazy
 /// companyNamesRDD.toDF().show()

  //min and max
  implicit val stockOrdering : Ordering[StockValue] = Ordering.fromLessThan( (a, b) => a.price < b.price )
  val minMsft = microsoftStocks.min()
///  println(minMsft)

  //reduce
  numbersRDD.reduce( _ + _)

  //grouping
  private val groupedStocksRDD: RDD[(String, Iterable[StockValue])] = stocksRDD.groupBy(_.symbol)

  val repartitionedSTocksRDD = stocksRDD.repartition(30)

  /**
    * repartitioning is expensive,it involves shuffling
    * Best practice:Partitione arly and then process
    * Ideal partition size:10-100 mb

  repartitionedSTocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  val coalescedRDD = moviesRDD.coalesce(15)
  coalescedRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

    */


}
