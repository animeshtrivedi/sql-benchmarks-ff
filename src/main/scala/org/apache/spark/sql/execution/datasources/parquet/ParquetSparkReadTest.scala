package org.apache.spark.sql.execution.datasources.parquet

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkFileFormatTest}

/**
  * Created by atr on 31.10.17.
  */
class ParquetSparkReadTest (fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  private val fileFormat = new ParquetFileFormat()
  private val rdd = transformFilesToRDD(fileFormat, fileFormat.buildReaderWithPartitionValues)

  override def execute(): String = {
    rdd.foreach(fx => {
      val filePart = PartitionedFile(InternalRow.empty, fx._2, 0, fx._3)
      val itr = fx._1(filePart)
      var rowsPerWorker = 0L
      while (itr.hasNext) {
        itr.next()
        rowsPerWorker += 1L
      }
      totalRowsAcc.add(rowsPerWorker)
    })
    "ParquetSparkTest " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ParquetSparkTest test "
}
