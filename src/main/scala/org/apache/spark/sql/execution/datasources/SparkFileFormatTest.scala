package org.apache.spark.sql.execution.datasources

import com.ibm.crail.benchmarks.fio.{FIOTest, FIOUtils}
import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.LongAccumulator

/**
  * Created by atr on 30.10.17.
  *
  * This is the base class which is suppose to implement the generic template from the file system to the
  * iterator interface. The logic here is simple. The driver needs to enumerate the file and executors
  * need to consume them.
  */
abstract class SparkFileFormatTest(fioOptions:FIOOptions, spark:SparkSession)  extends FIOTest {

  protected val filesEnumerated:List[(String, Long)] = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  var totalBytesExpected:Long  = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  def rdd:RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] = ???

  def transformFilesToRDD(fileFormat:FileFormat,
                                  func:(SparkSession,
                                    StructType,
                                    StructType,
                                    StructType,
                                    Seq[Filter],
                                    Map[String,String],
                                    Configuration)=>(PartitionedFile => Iterator[InternalRow]))
  :RDD[((PartitionedFile) => Iterator[InternalRow] , String, Long)] = {
    val conf = new Configuration()
    val list = filesEnumerated.map(fx => {
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val fileStatus = fs.getFileStatus(path)
      val schema = fileFormat.inferSchema(spark,
        Map[String, String](),
        Seq(fileStatus)).get
      import collection.JavaConversions._
      (func(spark,
          schema,
          new StructType(),
          schema,
          Seq[Filter](),
          fioOptions.getInputFormatOptions.toMap,
          conf),
        fx._1,
        fx._2
      )
    })
    spark.sparkContext.parallelize(list, fioOptions.getNumTasks)
  }

  protected val iotimeAcc:LongAccumulator = spark.sparkContext.longAccumulator("iotime")
  protected val setuptimeAcc:LongAccumulator = spark.sparkContext.longAccumulator("setuptime")
  protected val totalRowsAcc:LongAccumulator = spark.sparkContext.longAccumulator("totalRows")

  override def execute(): String = {
    this.rdd.foreach(fx => {
      val s1 = System.nanoTime()
      val filePart = PartitionedFile(InternalRow.empty, fx._2, 0, fx._3)
      val itr = fx._1(filePart)
      var rowsPerWorker = 0L
      val s2 = System.nanoTime()
      while (itr.hasNext) {
        itr.next()
        rowsPerWorker += 1L
      }
      val s3 = System.nanoTime()
      totalRowsAcc.add(rowsPerWorker)
      setuptimeAcc.add(s2 - s1)
      iotimeAcc.add(s3 - s2)
    })
    fioOptions.getTestName + ":" + fioOptions.getSparkFormat + " " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value
  }

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
  }
}
