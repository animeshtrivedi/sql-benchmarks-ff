package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.impl.{ColumnReadStoreImpl, ColumnReaderImpl}
import org.apache.parquet.column.page.{PageReadStore, PageReader}
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO}
import org.apache.parquet.schema.MessageType
import org.apache.parquet.tools.read.{SimpleReadSupport, SimpleRecord}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 27.10.17.
  */
class ParquetAloneTest(fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val filesEnumerated = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  println(filesEnumerated)
  var totalBytesExpected = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  private val iotimeAcc = spark.sparkContext.longAccumulator("iotime")
  private val setuptimeAcc = spark.sparkContext.longAccumulator("setuptime")
  private val totalRowsAcc = spark.sparkContext.longAccumulator("totalRows")
  private val rowBatchesAcc = spark.sparkContext.longAccumulator("rowBatches")

  private val rdd = spark.sparkContext.parallelize(filesEnumerated, fioOptions.getParallelism)
  private val mode = 1

  override final def execute(): String = if(fioOptions.getParquetAloneVersion == 1) executeV1() else executeV2()

  def executeV3():String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx._1)
      val readFooter:ParquetMetadata = ParquetFileReader.readFooter(conf,
        path,
        ParquetMetadataConverter.NO_FILTER)
      val mdata = readFooter.getFileMetaData
      val schema:MessageType = mdata.getSchema
      val colDesc = schema.getColumns
      var pages:PageReadStore = null
      val parquetFileReader = ParquetFileReader.open(conf, path)
      val expectedRows = parquetFileReader.getRecordCount
      var rowBatchesx = 0L
      var readSoFarRows = 0L
      val s2 = System.nanoTime()
      try
      {
        var contx = true
        while (contx) {
          pages = parquetFileReader.readNextRowGroup()
          if (pages != null) {
            rowBatchesx+=1
            ???
          } else {
            contx = false
          }
        }
      }
      catch
        {
          case foo: Exception => foo.printStackTrace()
        }
      finally {
        val s3 = System.nanoTime()
        parquetFileReader.close()
        val s4 = System.nanoTime()
        iotimeAcc.add(s3 - s2)
        setuptimeAcc.add(s2 - s1)
        setuptimeAcc.add(s4 - s3)
        totalRowsAcc.add(readSoFarRows)
        rowBatchesAcc.add(rowBatchesx)
        require(readSoFarRows == expectedRows, " readSoFar " + readSoFarRows + " and expectedRows " + expectedRows + " do not match ")
      }
    })

    "ParquetAloneTest V3 " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value +
      " , rowBatch " + rowBatchesAcc.value +
      " , expected parquet RowGroup size is : " + (totalBytesExpected / rowBatchesAcc.value)
  }

  //https://www.programcreek.com/java-api-examples/index.php?api=parquet.hadoop.ParquetFileReader
  // http://www.jofre.de/?p=1459
  def executeV2(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx._1)
      val readFooter:ParquetMetadata = ParquetFileReader.readFooter(conf,
        path,
        ParquetMetadataConverter.NO_FILTER)
      val schema:MessageType = readFooter.getFileMetaData.getSchema
      var pages:PageReadStore = null
      val parquetFileReader = ParquetFileReader.open(conf, path)
      val expectedRows = parquetFileReader.getRecordCount
      var rowBatchesx = 0L
      var readSoFarRows = 0L
      val s2 = System.nanoTime()
      try
      {
        var contx = true
        while (contx) {
          pages = parquetFileReader.readNextRowGroup()
          if (pages != null) {
            rowBatchesx+=1
            val rows = pages.getRowCount
            var columnIO: MessageColumnIO = new ColumnIOFactory().getColumnIO(schema)
            val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
            for (i <- 0L until rows) {
              val encodedRow = recordReader.read()
              // here we can convert it to raw values
              readSoFarRows+=1
            }
          } else {
            contx = false
          }
        }
      }
      catch
        {
          case foo: Exception => foo.printStackTrace()
        }
      finally {
        val s3 = System.nanoTime()
        parquetFileReader.close()
        val s4 = System.nanoTime()
        iotimeAcc.add(s3 - s2)
        setuptimeAcc.add(s2 - s1)
        setuptimeAcc.add(s4 - s3)
        totalRowsAcc.add(readSoFarRows)
        rowBatchesAcc.add(rowBatchesx)
        require(readSoFarRows == expectedRows, " readSoFar " + readSoFarRows + " and expectedRows " + expectedRows + " do not match ")
      }
    })

    "ParquetAloneTest V2 " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value +
      " , rowBatch " + rowBatchesAcc.value +
      " , expected parquet RowGroup size is : " + (totalBytesExpected / rowBatchesAcc.value)
  }

  def executeV1(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx._1)
      val reader = new ParquetReader[SimpleRecord](path, new SimpleReadSupport())
      var readSoFarRows = 0L
      val s2 = System.nanoTime()
      try {
        var contx = true
        while (contx) {
          val record = reader.read
          if (record != null) {
            readSoFarRows += 1
          } else {
            // break loop
            contx = false
          }
        }
      } catch
        {
          case foo: Exception => foo.printStackTrace()
        }
      finally {
        val s3 = System.nanoTime()
        reader.close()
        val s4 = System.nanoTime()
        iotimeAcc.add(s3 - s2)
        setuptimeAcc.add(s2 - s1)
        setuptimeAcc.add(s4 - s3)
        totalRowsAcc.add(readSoFarRows)
      }
    })

    "ParquetAloneTest v1 " + filesEnumerated.size +
      " HDFS files in " + fioOptions.getInputLocations +
      " directory (total bytes " + totalBytesExpected +
      " ), total rows " + totalRowsAcc.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "ParquetAloneTest test "

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected * 8L, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotimeAcc.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptimeAcc.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
    //    +"Average, io time/stage : " + Utils.decimalRound(ioTime/fioOptions.getNumTasks.toDouble) +
    //      " msec | setuptime " + Utils.decimalRound(setupTime/fioOptions.getNumTasks.toDouble) + " msec\n"+
    //    "NOTE: keep in mind that if tasks > #cpus_in_the_cluster then you need to adjust the average time\n"
  }
}

// http://www.jofre.de/?p=1459
//
//package de.jofre.test;
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.column.page.PageReadStore;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
//import org.apache.parquet.format.converter.ParquetMetadataConverter;
//import org.apache.parquet.hadoop.ParquetFileReader;
//import org.apache.parquet.hadoop.metadata.ParquetMetadata;
//import org.apache.parquet.io.ColumnIOFactory;
//import org.apache.parquet.io.MessageColumnIO;
//import org.apache.parquet.io.RecordReader;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.Type;
//
//public class Main {
//
//  private static Path path = new Path("file:\\C:\\myfile.snappy.parquet");
//
//  private static void printGroup(Group g) {
//    int fieldCount = g.getType().getFieldCount();
//    for (int field = 0; field &lt; fieldCount; field++) {
//      int valueCount = g.getFieldRepetitionCount(field);
//
//      Type fieldType = g.getType().getType(field);
//      String fieldName = fieldType.getName();
//
//      for (int index = 0; index &lt; valueCount; index++) {
//        if (fieldType.isPrimitive()) {
//          System.out.println(fieldName + " " + g.getValueToString(field, index));
//        }
//      }
//    }
//    System.out.println("");
//  }
//
//  public static void main(String[] args) throws IllegalArgumentException {
//
//    Configuration conf = new Configuration();
//
//    try {
//      ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
//      MessageType schema = readFooter.getFileMetaData().getSchema();
//      ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);
//
//      PageReadStore pages = null;
//      try {
//        while (null != (pages = r.readNextRowGroup())) {
//          final long rows = pages.getRowCount();
//          System.out.println("Number of rows: " + rows);
//
//          final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
//          final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
//          for (int i = 0; i &lt; rows; i++) {
//            final Group g = recordReader.read();
//            printGroup(g);
//
//            // TODO Compare to System.out.println(g);
//          }
//        }
//      } finally {
//        r.close();
//      }
//    } catch (IOException e) {
//      System.out.println("Error reading parquet file.");
//      e.printStackTrace();
//    }
//  }
//}
