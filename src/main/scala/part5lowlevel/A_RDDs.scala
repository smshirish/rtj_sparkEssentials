package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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

  /// stocksRDD.toDF().show()

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
  fromRDD_DS.show()



}
