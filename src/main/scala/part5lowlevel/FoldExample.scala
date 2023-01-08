package part5lowlevel

import org.apache.spark.sql.SparkSession

object FoldExample extends App{


  val spark = SparkSession.builder()
    .appName("RDD Exercises -1 ")
    .config("spark.master","local")
    .getOrCreate()


  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 30),
    ("C", 40), ("B", 30), ("B", 60)))

  println(
    "Total : "+
      inputRDD.fold(("",0)) ((acc,ele)=>{ ("Total", acc._2 + ele._2)  })
  )
 // println("Min : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Min", acc._2 min ele._2)  }))
 // println("Max : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Max", acc._2 max ele._2)  }))

}
