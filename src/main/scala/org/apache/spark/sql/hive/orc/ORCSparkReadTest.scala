package org.apache.spark.sql.hive.orc

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkFileFormatTest}

/**
  * Created by atr on 30.10.17.
  */
class ORCSparkReadTest(fioOptions:FIOOptions, spark:SparkSession) extends SparkFileFormatTest(fioOptions, spark) {
  /* here we do file format specific initialization */
  private val fileFormat = new OrcFileFormat()
  private val rdd = transformFilesToRDD(fileFormat, fileFormat.buildReader, fioOptions.getParallelism)

  override def execute(): String = {
    rdd.foreach(fx => {
      val filePart = PartitionedFile(InternalRow.empty, fx._2, 0, fx._3)
      val itr = fx._1(filePart)
      var x = 0L
      while (itr.hasNext) {
        itr.next()
        x += 1L
      }
    })
    "ORCSparkTest " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ORCSparkTest test "
}



//  rdd.foreach(fx =>{
//    val s1 = System.nanoTime()
//    val conf = new Configuration()
//    val path = new Path(fx._1)
//    val uri = path.toUri
//    val schema = OrcFileOperator.readSchema(Seq(uri.toString), Some(conf)).get
//
//    val orcRecordReader = {
//      val orcReader = OrcFile.createReader(path, OrcFile.readerOptions(conf))
//      new SparkOrcNewRecordReader(orcReader, conf, 0, fx._2)
//    }
//    val recordsIterator = new RecordReaderIterator[OrcStruct](orcRecordReader)
//    val readerItr = OrcRelation.unwrapOrcStructs(
//      conf,
//      schema,
//      Some(orcRecordReader.getObjectInspector.asInstanceOf[StructObjectInspector]),
//      recordsIterator)
//
//    val s2 = System.nanoTime()
//    var rowsx = 0L
//    while(readerItr.hasNext){
//      readerItr.next().asInstanceOf[UnsafeRow]
//      rowsx+=1
//    }
//    val s3 = System.nanoTime()
//
//    iotimeAcc.add(s3 -s2)
//    setuptimeAcc.add(s2 -s1)
//    totalRowsAcc.add(rowsx)
//  })
//
//  "ORCSparkTest " + filesEnumerated.size +
//    " HDFS files in " + fioOptions.getInputLocations +
//    " directory (total bytes " + totalBytesExpected +
//    " ), total rows " + totalRowsAcc.value
//
//}