package com.fullcontact.interview


import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().appName("FullContact Data Processing").master("local").getOrCreate()
  }

}


class DatasetSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  it("aliases a DataFrame") {

    val records = Seq(
      "YSJNPIT ORMJLXN OSMHWUF ANVLVIH",
      "RERONRV VQZAHIV EKTDXLG YSJNPIT",
      "JFOCIBQ WYQWIDV YSJNPIT GQMCOEC NSZGNUC"
    ).toDS()

    val query = Seq(
      "YSJNPIT"
    ).toDS()

    val mainOutput = RecordFinder.getMatchingRecords(spark,records,query).toDF()

    val expectedDF = Seq(
      "YSJNPIT:YSJNPIT ORMJLXN OSMHWUF ANVLVIH",
      "YSJNPIT:RERONRV VQZAHIV EKTDXLG YSJNPIT",
      "YSJNPIT:JFOCIBQ WYQWIDV YSJNPIT GQMCOEC NSZGNUC"
    ).toDF()

    assertSmallDatasetEquality(mainOutput, expectedDF,orderedComparison = false)

  }
}
