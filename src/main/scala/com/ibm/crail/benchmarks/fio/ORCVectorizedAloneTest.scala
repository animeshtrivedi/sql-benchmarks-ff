package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{DecimalColumnVector, LongColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, RecordReader}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 03.11.17.
  */
class ORCVectorizedAloneTest (fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {
  //MORE: https://issues.apache.org/jira/browse/ORC-72
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
      val conf: Configuration = new Configuration()
      val path = new Path(fx._1)
      val reader = OrcFile.createReader(path,
        OrcFile.readerOptions(conf))
      val rows:RecordReader = reader.rows()
      val batch:VectorizedRowBatch = reader.getSchema.createRowBatch()
      var rowCount = 0L
      val s2 = System.nanoTime()
      while (rows.nextBatch(batch)) {
        val colsArr = batch.cols
        val intVector0: LongColumnVector = colsArr(0).asInstanceOf[LongColumnVector]
        val intVector1: LongColumnVector = colsArr(1).asInstanceOf[LongColumnVector]
        val intVector2: LongColumnVector = colsArr(2).asInstanceOf[LongColumnVector]
        val intVector3: LongColumnVector = colsArr(3).asInstanceOf[LongColumnVector]
        val intVector4: LongColumnVector = colsArr(4).asInstanceOf[LongColumnVector]
        val intVector5: LongColumnVector = colsArr(5).asInstanceOf[LongColumnVector]
        val intVector6: LongColumnVector = colsArr(6).asInstanceOf[LongColumnVector]
        val intVector7: LongColumnVector = colsArr(7).asInstanceOf[LongColumnVector]
        val intVector8: LongColumnVector = colsArr(8).asInstanceOf[LongColumnVector]
        // +9 ints = 9
        val intVector9: LongColumnVector = colsArr(9).asInstanceOf[LongColumnVector]
        // +1 long = 10
        val intVector10: LongColumnVector = colsArr(10).asInstanceOf[LongColumnVector]
        // +1 int = 11
        val decimalVector0: DecimalColumnVector = colsArr(11).asInstanceOf[DecimalColumnVector]
        val decimalVector1: DecimalColumnVector = colsArr(12).asInstanceOf[DecimalColumnVector]
        val decimalVector2: DecimalColumnVector = colsArr(13).asInstanceOf[DecimalColumnVector]
        val decimalVector3: DecimalColumnVector = colsArr(14).asInstanceOf[DecimalColumnVector]
        val decimalVector4: DecimalColumnVector = colsArr(15).asInstanceOf[DecimalColumnVector]
        val decimalVector5: DecimalColumnVector = colsArr(16).asInstanceOf[DecimalColumnVector]
        val decimalVector6: DecimalColumnVector = colsArr(17).asInstanceOf[DecimalColumnVector]
        val decimalVector7: DecimalColumnVector = colsArr(18).asInstanceOf[DecimalColumnVector]
        val decimalVector8: DecimalColumnVector = colsArr(19).asInstanceOf[DecimalColumnVector]
        val decimalVector9: DecimalColumnVector = colsArr(20).asInstanceOf[DecimalColumnVector]
        val decimalVector10: DecimalColumnVector = colsArr(21).asInstanceOf[DecimalColumnVector]
        val decimalVector11: DecimalColumnVector = colsArr(22).asInstanceOf[DecimalColumnVector]
        // +12 decimal = 23 columns
        for (i <- 0 until batch.size) {
          val intVal0 = intVector0.vector(i)
          val intVal1 = intVector1.vector(i)
          val intVal2 = intVector2.vector(i)
          val intVal3 = intVector3.vector(i)
          val intVal4 = intVector4.vector(i)
          val intVal5 = intVector5.vector(i)
          val intVal6 = intVector6.vector(i)
          val intVal7 = intVector7.vector(i)
          val intVal8 = intVector8.vector(i)
          val intVal9 = intVector9.vector(i)

          val intVal10 = intVector10.vector(i)

          val decimalVal0 = decimalVector0.vector(i)
          val decimalVal1 = decimalVector1.vector(i)
          val decimalVal2 = decimalVector2.vector(i)
          val decimalVal3 = decimalVector3.vector(i)
          val decimalVal4 = decimalVector4.vector(i)
          val decimalVal5 = decimalVector5.vector(i)
          val decimalVal6 = decimalVector6.vector(i)
          val decimalVal7 = decimalVector7.vector(i)
          val decimalVal8 = decimalVector8.vector(i)
          val decimalVal9 = decimalVector9.vector(i)
          val decimalVal10 = decimalVector10.vector(i)
          val decimalVal11 = decimalVector11.vector(i)

          rowCount += 1
        }
      }
      val s3 = System.nanoTime()
      rows.close()
      iotimeAcc.add(s3 - s2)
      setuptimeAcc.add(s2 - s1)
      totalRowsAcc.add(rowCount)
    })
    "ORCVectorizedAloneTest " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ORCAloneTest test "

  override def printAdditionalInformation(timelapsedinNanosec: Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n" +
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
  }
}