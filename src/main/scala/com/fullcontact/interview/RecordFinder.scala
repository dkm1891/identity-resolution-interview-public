package com.fullcontact.interview

import org.apache.spark.sql.{ DataFrame, Dataset, Row, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

object RecordFinder {

  def getMatchingRecords(spark: SparkSession, records: Dataset[String], queries: Dataset[String]): DataFrame =  {
    import spark.implicits._

    val recordsDF = records.select(split(col("value"), " ").as("records")).drop("value")

    var result = queries.join(recordsDF, expr("array_contains(records,value)"))

    result = result.withColumn("result", concat(col("value"), lit(":"), concat_ws(" ", col("records"))))

    result
      .drop("value", "records")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .text("matchingRecords")

    val concatExpr = concat_ws(" ", flatten(collect_list("records")))

    val resultUnion = result.groupBy(col("value"))
      .agg(concat(col("value"), lit(":"), concatExpr).alias("result"))

    resultUnion
      .drop("value")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .text("matchingRecordsUnion")

    val resultDistinct = result.groupBy(col("value"))
      .agg(concat(col("value"), lit(":"), concat_ws(" ", array_distinct(split(concatExpr, " ")))).alias("result"))

    resultDistinct
      .drop("value")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .text("matchingRecordsDistinct")
      
    result.drop("value", "records")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FullContact Data Processing").master("local").getOrCreate()

    import spark.implicits._

    val records = spark.read.textFile("Records.txt")

    val queries = spark.read.textFile("Queries.txt").filter("value == 'YSJNPIT'")

    val outputDF = getMatchingRecords(spark, records, queries);

  }
}