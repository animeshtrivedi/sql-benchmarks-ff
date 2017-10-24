package org.apache.spark.sql

import com.ibm.crail.benchmarks.{BaseTest, FIOOptions, Utils}

/**
  * Created by atr on 13.10.17.
  */
class IteratorReadTest (fioOptions:FIOOptions, spark:SparkSession) extends BaseTest {

  private val rdd = spark.sparkContext.parallelize(Range(0, fioOptions.getNumTasks),
    fioOptions.getParallelism)
  private val options = fioOptions.getInputFormatOptions
  if(options.size() == 0){
    println("Warning: No options found - adding the default I have")
    options.put("inputrows", "1000000")
    options.put("payloadsize", "4096")
    options.put("intrange", "1000000")
    options.put("schema", "IntWithPayload")
  }

  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")
  private val totalRows = spark.sparkContext.longAccumulator("totalRows")

  override def execute(): String = {
    rdd.foreach(p =>{
      val s1 = System.nanoTime()
      val nullFS = new NullFileFormat()
      import collection.JavaConversions._
      nullFS.setSchema(options.toMap)
      val itr = nullFS.buildIterator(options.toMap)
      var rowsx = 0L
      val s2 = System.nanoTime()
      while(itr.hasNext){
        val row = itr.next()
        rowsx+=1
      }
      val s3 = System.nanoTime()
      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      totalRows.add(rowsx)
    })
    "IteratorRead<int,payload> read " + totalRows.value + " rows "
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "IteratorRead test"

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val totalBytesExpected = totalRows.value * (4096 + 4)
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected, timelapsedinNanosec)
    val bwItr = Utils.twoLongDivToDecimal(totalRows.value, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotime.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptime.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps or " + bwItr + " itr/sec \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
    //    +"Average, io time/stage : " + Utils.decimalRound(ioTime/fioOptions.getNumTasks.toDouble) +
    //      " msec | setuptime " + Utils.decimalRound(setupTime/fioOptions.getNumTasks.toDouble) + " msec\n"+
    //    "NOTE: keep in mind that if tasks > #cpus_in_the_cluster then you need to adjust the average time\n"
  }

}
