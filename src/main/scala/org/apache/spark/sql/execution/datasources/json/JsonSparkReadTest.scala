package org.apache.spark.sql.execution.datasources.json

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkFileFormatTest}

/**
  * Created by atr on 31.10.17.
  */
class JsonSparkReadTest (fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  private val fileFormat = new JsonFileFormat()

  override final val rdd:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] =
    transformFilesToRDD(fileFormat, fileFormat.buildReader)

  override def explain(): Unit = {}

  override def plainExplain(): String = "JsonSparkTest test "
}

