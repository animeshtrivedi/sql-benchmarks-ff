package org.apache.spark.sql

import com.ibm.crail.benchmarks.fio.FIOTest
import com.ibm.crail.benchmarks.{FIOOptions, Utils}

/**
  * Created by atr on 13.10.17.
  */
class IteratorReadTest (fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val rdd = spark.sparkContext.parallelize(Range(0, fioOptions.getNumTasks),
    fioOptions.getParallelism)
  private val options = fioOptions.getInputFormatOptions
  if(options.size() == 0){
    println("**** Warning:**** No options found - adding the default that I have")
    options.put(NullFileFormat.KEY_INPUT_ROWS, "1000000")
    options.put(NullFileFormat.KEY_PAYLOAD_SIZE, "4096")
    options.put(NullFileFormat.KEY_INT_RANGE, "1000000")
    options.put(NullFileFormat.KEY_SCHEMA, "IntWithPayload")
  }

  private val iotimeAcc = spark.sparkContext.longAccumulator("iotime")
  private val setuptimeAcc = spark.sparkContext.longAccumulator("setuptime")
  private val totalRowsAcc = spark.sparkContext.longAccumulator("totalRows")

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
      iotimeAcc.add(s3 -s2)
      setuptimeAcc.add(s2 -s1)
      totalRowsAcc.add(rowsx)
    })
    "IteratorRead " + options.get(NullFileFormat.KEY_SCHEMA) + " read " + totalRowsAcc.value + " rows "
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "IteratorRead test"

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val rowSize = {
      val x = new NullFileFormat()
      import collection.JavaConversions._
      x.setSchema(options.toMap)
      x.getSchemaRowSize(options.toMap)
    }
    val totalBitsExpected = totalRowsAcc.value * rowSize * 8L
    val bw = Utils.twoLongDivToDecimal(totalBitsExpected, timelapsedinNanosec)
    val bwItr = Utils.twoLongDivToDecimal(totalRowsAcc.value, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps or " + bwItr + " Gitr/sec \n"+
      "Total, io time         : " + ioTime + " msec, setuptime " + setupTime + " msec, rowSize : " + rowSize + " bytes | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
    //    +"Average, io time/stage : " + Utils.decimalRound(ioTime/fioOptions.getNumTasks.toDouble) +
    //      " msec | setuptime " + Utils.decimalRound(setupTime/fioOptions.getNumTasks.toDouble) + " msec\n"+
    //    "NOTE: keep in mind that if tasks > #cpus_in_the_cluster then you need to adjust the average time\n"
  }

}
