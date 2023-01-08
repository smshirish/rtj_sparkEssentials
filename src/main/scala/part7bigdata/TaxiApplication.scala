package part7bigdata

import org.apache.spark.sql.SparkSession

object TaxiApplication extends App{

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Taxi Big Data App")
    .getOrCreate()

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018") //data in default parquet format. so load directly

  taxiDF.printSchema()
  ///println(taxiDF.count())
  /**
    * root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
    */

  val taxiZonesDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/taxi_zones.csv")

 /// taxiZonesDF.printSchema()
 /// println(taxiZonesDF.count())

  /**
    * root
 |-- LocationID: integer (nullable = true)
 |-- Borough: string (nullable = true)
 |-- Zone: string (nullable = true)
 |-- service_zone: string (nullable = true)
    *
    */

  /**
    * Exercises:
    * Which zones have the most pickups and drop offs overall?
    * What are the peak hours for taxi?
    * How are the trips distributed ? Whay are the people taking the cab?
    * What are the peak hours for long and short trips?
    * What are the top 3 popular pickup/drop off zones for long/short trips
    * How are people paying for the ride on long and short trips?
    * How is payment evolving over time?
    * Can we explore ride sharing opurtunities by grouping close short rides?
    */
}
