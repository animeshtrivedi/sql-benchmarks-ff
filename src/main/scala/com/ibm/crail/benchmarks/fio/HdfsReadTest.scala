package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 11.10.17.
  */

class HdfsReadTest(fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val filesEnumerated = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  println(filesEnumerated)
  var totalBytesExpected = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  private val totalBytesRead = spark.sparkContext.longAccumulator("totalBytesRead")
  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")

  private val requestSize = fioOptions.getRequetSize
  private val align = fioOptions.getAlign
  private val rdd = spark.sparkContext.parallelize(filesEnumerated, fioOptions.getParallelism)


  override def execute(): String = if(fioOptions.isUseFully) executeFully() else executeRead()

  def executeFully(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val istream = fs.open(path)
      val buffer = new Array[Byte](requestSize - align)
      var readSoFar = 0L
      var toRead = 0

      val s2 = System.nanoTime()
      /* parquet uses readfull code, hence we are using it here too.
      ParquetFileReader.readAll()
       */
      while (readSoFar < fx._2){
        val leftInFile = fx._2 - readSoFar
        toRead = Math.min(if(leftInFile > Integer.MAX_VALUE ) Integer.MAX_VALUE else leftInFile.toInt,
          buffer.length)
        istream.readFully(buffer, 0, toRead)
        readSoFar+=toRead
      }
      val s3 = System.nanoTime()
      istream.close()
      val s4 = System.nanoTime()

      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      setuptime.add(s4 -s3)
      totalBytesRead.add(readSoFar)
    })
    require(totalBytesExpected == totalBytesRead.value,
      " Expected ( " + totalBytesExpected + " ) and read ( "+totalBytesRead.value+" ) bytes do not match ")
    "ReadFully " + filesEnumerated.size + " HDFS files in " + fioOptions.getInputLocations + " directory, total size " + totalBytesRead.value + " bytes, align " + align
  }

  def executeRead(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val istream = fs.open(path)
      val buffer = new Array[Byte](requestSize - align)
      var readSoFar = 0L

      val s2 = System.nanoTime()
      while (readSoFar < fx._2){
        readSoFar+=istream.read(buffer)
      }
      val s3 = System.nanoTime()
      istream.close()
      val s4 = System.nanoTime()

      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      setuptime.add(s4 -s3)
      totalBytesRead.add(readSoFar)
    })
    require(totalBytesExpected == totalBytesRead.value,
      " Expected ( " + totalBytesExpected + " ) and read ( "+totalBytesRead.value+" ) bytes do not match ")
    "Read w/o Fully " + filesEnumerated.size + " HDFS files in " + fioOptions.getInputLocations + " directory, total size " + totalBytesRead.value + " bytes, align " + align
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "Hdfs read test"

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
