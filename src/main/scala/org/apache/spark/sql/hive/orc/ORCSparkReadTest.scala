package org.apache.spark.sql.hive.orc

import java.net.URI

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import com.ibm.crail.benchmarks.fio.{FIOTest, FIOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, OrcStruct, SparkOrcNewRecordReader}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.sources.Filter

/**
  * Created by atr on 30.10.17.
  */
class ORCSparkReadTest(fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

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
  //private val rdd2 = spark.sparkContext.parallelize(transform(), fioOptions.getParallelism)

  private def transform():List[Iterator[InternalRow]] = {
    filesEnumerated.map(fx=> {
      val conf = new Configuration()
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val fileStatus = fs.getFileStatus(path)
      val orcfileSupprt = new OrcFileFormat()
      val schema = orcfileSupprt.inferSchema(spark,
              Map[String, String](),
              Seq(fileStatus)).get
      val filePart = PartitionedFile(InternalRow.empty, fx._1, 0, fx._2)
      orcfileSupprt.buildReader(spark,
        schema,
        null,
        schema,
        Seq[Filter](),
        Map[String, String](),
        conf)(filePart)
    })
  }

  override def execute(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val fileStatus = fs.getFileStatus(path)
      val orcfileSupprt = new OrcFileFormat()
      val schema = OrcFileOperator.readSchema(Seq(uri.toString), Some(conf)).get
      val filePart = PartitionedFile(InternalRow.empty, fx._1, 0, fx._2)

      val orcRecordReader = {
        val job = Job.getInstance(conf)
        FileInputFormat.setInputPaths(job, filePart.filePath)

        val fileSplit = new FileSplit(
          new Path(new URI(filePart.filePath)), filePart.start, filePart.length, Array.empty
        )
        // Custom OrcRecordReader is used to get
        // ObjectInspector during recordReader creation itself and can
        // avoid NameNode call in unwrapOrcStructs per file.
        // Specifically would be helpful for partitioned datasets.
        val orcReader = OrcFile.createReader(
          new Path(new URI(filePart.filePath)), OrcFile.readerOptions(conf))
        new SparkOrcNewRecordReader(orcReader, conf, fileSplit.getStart, fileSplit.getLength)
      }
      val recordsIterator = new RecordReaderIterator[OrcStruct](orcRecordReader)
      val readerItr = OrcRelation.unwrapOrcStructs(
        conf,
        schema,
        Some(orcRecordReader.getObjectInspector.asInstanceOf[StructObjectInspector]),
        recordsIterator)

      val s2 = System.nanoTime()
      var rowsx = 0L
      while(readerItr.hasNext){
        readerItr.next().asInstanceOf[UnsafeRow]
        rowsx+=1
      }
      val s3 = System.nanoTime()

      iotimeAcc.add(s3 -s2)
      setuptimeAcc.add(s2 -s1)
      totalRowsAcc.add(rowsx)
    })

    "ORCSparkTest " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value

  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ORCSparkTest test "

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
  }
}