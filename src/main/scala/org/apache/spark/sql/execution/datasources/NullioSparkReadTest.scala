package org.apache.spark.sql.execution.datasources

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{NullFileFormat, SparkSession}

/**
  * Created by atr on 31.10.17.
  */
class NullioSparkReadTest (fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  private val fileFormat = new NullFileFormat()
  private val options = fioOptions.getInputFormatOptions
  if(options.size() == 0){
    println("**** Warning:**** No options found - adding the default that I have")
    options.put(NullFileFormat.KEY_INPUT_ROWS, "1000000")
    options.put(NullFileFormat.KEY_PAYLOAD_SIZE, "4096")
    options.put(NullFileFormat.KEY_INT_RANGE, "1000000")
    options.put(NullFileFormat.KEY_SCHEMA, "IntWithPayload")
  }

  override final val rdd:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] =
    transformFilesToRDD(fileFormat, fileFormat.buildReader)

  override def explain(): Unit = {}

  override def plainExplain(): String = "NullIOSparkTest test "
}

