package org.apache.spark.sql.execution.datasources

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{NullFileFormat, SparkSession}

/**
  * Created by atr on 31.10.17.
  */
class NullioSparkReadTest (fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  var totalPerExecutorSize = 0L

  val nullRDD:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] = {
    /* we first have to make a list of empty file sizes */
    val fileFormat = new NullFileFormat()
    val options = fioOptions.getInputFormatOptions
    if(options.size() == 0){
      println("**** Warning:**** No options found - adding the default that I have")
      options.put(NullFileFormat.KEY_INPUT_ROWS, "1000000")
      options.put(NullFileFormat.KEY_PAYLOAD_SIZE, "4096")
      options.put(NullFileFormat.KEY_INT_RANGE, "1000000")
      options.put(NullFileFormat.KEY_SCHEMA, "IntWithPayload")
    }
    import collection.JavaConversions._
    val schema = fileFormat.inferSchema(spark, options.toMap, Seq()).get
    val conf = new Configuration()
    val func = fileFormat.buildReader(spark, schema, null, schema, Seq(), options.toMap, conf)
    val rowSize = fileFormat.getSchemaRowSize(options.toMap)
    totalPerExecutorSize = rowSize * options.get(NullFileFormat.KEY_INPUT_ROWS).toLong
    val lx = new Array[Tuple3[PartitionedFile => Iterator[InternalRow], String, Long]](fioOptions.getNumTasks)
    var i = 0
    while (i < fioOptions.getNumTasks){
      lx(i) = (func, "", totalPerExecutorSize)
      i+=1
    }
    spark.sparkContext.parallelize(lx, fioOptions.getNumTasks)
  }

  totalBytesExpected = totalPerExecutorSize * fioOptions.getNumTasks

  override final val rdd:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] = nullRDD

  override def explain(): Unit = {}

  override def plainExplain(): String = "NullIOSparkTest test "
}

