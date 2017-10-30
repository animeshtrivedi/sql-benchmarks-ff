package org.apache.spark.sql.execution.datasources

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import com.ibm.crail.benchmarks.fio.{FIOTest, FIOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

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

  def transformWithPartitionValue(fileFormat:FileFormat):List[(StructType, (PartitionedFile) => Iterator[InternalRow] , String, Long)] = {
    filesEnumerated.map(fx => {
      val conf = new Configuration()
      val path = new Path(fx._1)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val fileStatus = fs.getFileStatus(path)
      val schema = fileFormat.inferSchema(spark,
        Map[String, String](),
        Seq(fileStatus)).get
      (schema,
        fileFormat.buildReaderWithPartitionValues(spark,
          schema,
          new StructType(),
          schema,
          Seq[Filter](),
          Map[String, String](),
          conf),
        fx._1,
        fx._2
      )
    })
  }

  protected val iotimeAcc = spark.sparkContext.longAccumulator("iotime")
  protected val setuptimeAcc = spark.sparkContext.longAccumulator("setuptime")
  protected val totalRowsAcc = spark.sparkContext.longAccumulator("totalRows")

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
  }

  def getFileFormat:FileFormat

  protected lazy val rdd = spark.sparkContext.parallelize(transformWithPartitionValue(getFileFormat),
    fioOptions.getParallelism)
}
