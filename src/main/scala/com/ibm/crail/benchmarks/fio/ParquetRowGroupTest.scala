package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 24.10.17.
  */
class ParquetRowGroupTest (fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

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
      val conf = new Configuration()
      val path = new Path(fx._1)
      val parquetReader = ParquetFileReader.open(conf, path)
        //new ParquetFileReader(config, footer.getFileMetaData, file, blocks, requestedSchema.getColumns)
      val expectedRows = parquetReader.getRecordCount
      var readSoFarRows = 0L
      var rowBatchesx = 0L

      val s2 = System.nanoTime()
      while (readSoFarRows < expectedRows){
        val pageReadStore = parquetReader.readNextRowGroup()
        readSoFarRows+=pageReadStore.getRowCount
        rowBatchesx+=1
      }

      val s3 = System.nanoTime()
      parquetReader.close()
      val s4 = System.nanoTime()

      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      setuptime.add(s4 -s3)
      totalRows.add(readSoFarRows)
      rowBatches.add(rowBatchesx)
    })

    "ParquetFileReader.readNextRowGroup " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRows.value +
      " , rowBatch " + rowBatches.value +
      " , expected parquet RowGroup size is : " + (totalBytesExpected / rowBatches.value)
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ParquetFileReader.readNextRowGroup read test "

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