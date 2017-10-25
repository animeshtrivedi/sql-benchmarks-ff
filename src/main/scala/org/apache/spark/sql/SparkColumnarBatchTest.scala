package org.apache.spark.sql

import com.ibm.crail.benchmarks.fio.{FIOTest, FIOUtils}
import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.execution.vectorized.ColumnarBatch

/**
  * Created by atr on 24.10.17.
  */
class SparkColumnarBatchTest (fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val filesEnumerated = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  println(filesEnumerated)
  var totalBytesExpected = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")
  private val totalRows = spark.sparkContext.longAccumulator("totalRows")
  private val rowBatches = spark.sparkContext.longAccumulator("rowBatches")
  private val rdd = spark.sparkContext.parallelize(filesEnumerated, fioOptions.getParallelism)


  override def execute(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      /* from there on we use the generated code */
      import scala.collection.JavaConverters._
      val vectorizedReader = new VectorizedParquetRecordReader
      vectorizedReader.initialize(fx._1, List("intKey", "payload").asJava)
      vectorizedReader.enableReturningBatches()
      val recordIterator = new RecordReaderIterator(vectorizedReader).asInstanceOf[Iterator[InternalRow]]
      var rowsx = 0L
      var rowsBatchx = 0L
      val s2 = System.nanoTime()
      while(recordIterator.hasNext){
        val columnarBatch = recordIterator.next.asInstanceOf[ColumnarBatch]
        rowsx+=columnarBatch.numRows()
        rowsBatchx+=1
      }
      val s3 = System.nanoTime()
      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      totalRows.add(rowsx)
      rowBatches.add(rowsBatchx)
    })
    "SparkColumnarBatch : Parquet<int,payload> read " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRows.value +
      " , rowBatch " + rowBatches.value +
      " , expected Spark ColumarGroup size is (approx DEFAULT_BATCH_SIZE rows in ColumnarBatch): " + (totalRows.value / rowBatches.value)
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "SparkColumnarBatch <int,payload> reading test"

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotime.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptime.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
    //    +"Average, io time/stage : " + Utils.decimalRound(ioTime/fioOptions.getNumTasks.toDouble) +
    //      " msec | setuptime " + Utils.decimalRound(setupTime/fioOptions.getNumTasks.toDouble) + " msec\n"+
    //    "NOTE: keep in mind that if tasks > #cpus_in_the_cluster then you need to adjust the average time\n"
  }
}