package com.databricks.spark.avro

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkFileFormatTest}

/**
  * Created by atr on 01.11.17.
  */
class AvroSparkReadTest (fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  override final val rdd:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] = {
    val fileFormat = new DefaultSource()
    transformFilesToRDD(fileFormat, fileFormat.buildReader)
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "AvroSparkTest test "
}

