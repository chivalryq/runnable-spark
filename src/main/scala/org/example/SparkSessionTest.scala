package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object SparkSessionTest extends App {
  print(classOf[org.apache.commons.lang3.SystemUtils].getResource("SystemUtils.class"))
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("First SparkContext:")
  println("APP Name :" + spark.sparkContext.appName)
  println("Deploy Mode :" + spark.sparkContext.deployMode)
  println("Master :" + spark.sparkContext.master)

  val schema = StructType(
    List(
      StructField("RecordNumber", IntegerType, true),
      StructField("Zipcode", StringType, true),
      StructField("ZipCodeType", StringType, true),
      StructField("City", StringType, true),
      StructField("State", StringType, true),
      StructField("LocationType", StringType, true),
      StructField("Lat", StringType, true),
      StructField("Long", StringType, true),
      StructField("Xaxis", StringType, true),
      StructField("Yaxis", StringType, true),
      StructField("Zaxis", StringType, true),
      StructField("WorldRegion", StringType, true),
      StructField("Country", StringType, true),
      StructField("LocationText", StringType, true),
      StructField("Location", StringType, true),
      StructField("Decommisioned", StringType, true)
    )
  )
  // spark Session contains *context,like sql,stream..
  val ctx = spark.sqlContext
  println(ctx.tableNames().mkString("Array(", ", ", ")"))
  val df = spark.readStream
    .schema(schema) //Below codes provides example
    .json("/tmp/stream_folder")

  df.printSchema()

  val groupDF = df.select("Zipcode")
    .groupBy("Zipcode").count()
  groupDF.printSchema()

//  groupDF.writeStream
//    .format("console")
//    .outputMode("complete")
//    .start()
//    .awaitTermination()
}
