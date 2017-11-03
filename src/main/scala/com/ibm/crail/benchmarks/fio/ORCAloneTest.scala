package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 02.11.17.
  */
//https://github.com/animeshtrivedi/orc-bench/blob/master/pom.xml

class ORCAloneTest(fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val filesEnumerated = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  println(filesEnumerated)
  var totalBytesExpected = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  private val iotimeAcc = spark.sparkContext.longAccumulator("iotime")
  private val setuptimeAcc = spark.sparkContext.longAccumulator("setuptime")
  private val totalRowsAcc = spark.sparkContext.longAccumulator("totalRows")

  private val rdd = spark.sparkContext.parallelize(filesEnumerated, fioOptions.getParallelism)

  override final def execute(): String = {
    rdd.foreach(fx => {
      val s1 = System.nanoTime()
      val conf:Configuration = new Configuration()
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)

      // This one is from
      val reader:Reader = OrcFile.createReader(fs, path)
      val recordReader:RecordReader = OrcInputFormat.createReaderFromFile(reader, conf, 0, fx._2)
      val rowReader = reader.rows(null)
      var row:Object = null
      var rowCount = 0L
      var value:OrcStruct = null
      var vectorizedRowBatch:VectorizedRowBatch = null
      val s2 = System.nanoTime()
//      while (rowReader.hasNext) {
//        row = rowReader.next(row)
//        rowCount += 1
//      }
      while(recordReader.hasNext){
        recordReader.next(value)
        //vectorizedRowBatch = recordReader.nextBatch(vectorizedRowBatch)
        rowCount += 1
      }
      val s3 = System.nanoTime()
      rowReader.close()
      iotimeAcc.add(s3 - s2)
      setuptimeAcc.add(s2 - s1)
      totalRowsAcc.add(rowCount)
    })
    "ORCAloneTest " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ORCAloneTest test "

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
  }
}