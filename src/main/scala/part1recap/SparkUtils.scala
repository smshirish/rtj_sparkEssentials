package part1recap

import org.apache.spark.sql.{SaveMode, SparkSession}
import part4sql.C_3_SparkSqlExercises.sparkDBBasePath

import java.io.File

object SparkUtils {

  val SPARK_DB_PATH = "src/main/resources/warehouse"

  def readTable(spark:SparkSession, tableName: String) = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.$tableName" )
      .load()
  }

  def deleteDBTableDir(sparkDBBasePath:String, tableName:String ): Boolean = {
    //workaround  -> Delete the DB directory before creating the table again
    val dirToDelete = new File(sparkDBBasePath + "/rtjvm.db"+ "/" + tableName)
    if (dirToDelete.isDirectory) {
      dirToDelete.listFiles().foreach(file => file.delete());
      dirToDelete.delete();
    }
    else
      dirToDelete.delete()
  }

  def transferTables(spark:SparkSession,sparkDBBasePath:String,tableNames:List[String], shouldWrite :Boolean = false) =  tableNames.foreach{ tableName =>
    deleteDBTableDir(sparkDBBasePath,tableName)
    val tableDF = readTable(spark,tableName)
    //make table visible to spark SQL
    tableDF.createOrReplaceTempView(tableName)
    if (shouldWrite){
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }
}
