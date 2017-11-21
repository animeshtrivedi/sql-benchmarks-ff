/*
 * Spark Benchmarks
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.spark.sql

import com.ibm.crail.benchmarks.fio.{FIOTest, FIOUtils}
import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{GeneratedIteratorIntWithPayload, GeneratedIteratorStoreSalesNoWrite}

/**
  * Created by atr on 12.10.17.
  */
class ParquetReadTest(fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val filesEnumerated = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  println(filesEnumerated)
  var totalBytesExpected = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")
  private val totalRows = spark.sparkContext.longAccumulator("totalRows")
  private val rdd = spark.sparkContext.parallelize(filesEnumerated, fioOptions.getParallelism)


  private val columnNames = scala.collection.mutable.ListBuffer.empty[String]
  setColumnNames()

  private def setColumnNames():Unit = {
    val conf = new Configuration()
    val parquetReader = ParquetFileReader.open(conf, new Path(filesEnumerated.head._1))
    val schema = parquetReader.getFooter.getFileMetaData.getSchema
    val listPath = schema.getPaths
    for(a <- 0 until listPath.size()){
      val arr = listPath.get(a)
      arr.foreach(p => {
        columnNames += p
      })
    }
    println("Detected Column names: " + columnNames)
  }

  private val table = if(columnNames.size == 2) "IntWithPayload" else "store_sales"


  override def execute(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      /* from there on we use the generated code */
      import scala.collection.JavaConverters._
      val vectorizedReader = new VectorizedParquetRecordReader

      vectorizedReader.initialize(fx._1, columnNames.asJava)
      vectorizedReader.enableReturningBatches()
      val recordIterator = new RecordReaderIterator(vectorizedReader).asInstanceOf[Iterator[InternalRow]]
      val objArr = new Array[Object](2)
      // these are dummy SQL metrics we can remove them eventually
      objArr(0) = new SQLMetric("atr1", 0L)
      objArr(1) = new SQLMetric("atr1", 0L)

      val generatedIterator = if(columnNames.size == 2) {
        new GeneratedIteratorIntWithPayload(objArr)
      } else {
        new GeneratedIteratorStoreSalesNoWrite(objArr)
      }
      generatedIterator.init(0, Array(recordIterator))
      var rowsx = 0L
      val s2 = System.nanoTime()
      while(generatedIterator.hasNext){
        val rows = generatedIterator.next().asInstanceOf[UnsafeRow]
        rowsx+=1
      }
      val s3 = System.nanoTime()
      println(generatedIterator.asInstanceOf[GeneratedIteratorStoreSalesNoWrite].getSum)
      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      totalRows.add(rowsx)
    })
    "Parquet " + table + " read " + filesEnumerated.size + " HDFS files in " + fioOptions.getInputLocations + " directory (total bytes " + totalBytesExpected + " ), total rows " + totalRows.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "Parquet " + table + " reading test"

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
