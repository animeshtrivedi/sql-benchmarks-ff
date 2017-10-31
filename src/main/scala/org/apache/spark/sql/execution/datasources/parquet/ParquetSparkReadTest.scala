package org.apache.spark.sql.execution.datasources.parquet

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkFileFormatTest}

/**
  * Created by atr on 31.10.17.
  */
class ParquetSparkReadTest (fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  private val fileFormat = new ParquetFileFormat()
  override final val rdd:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] =
    transformFilesToRDD(fileFormat, fileFormat.buildReaderWithPartitionValues)

  override def explain(): Unit = {}

  override def plainExplain(): String = "ParquetSparkTest test \n WARNING: This shows the performance of reading columnarBatch."
}
