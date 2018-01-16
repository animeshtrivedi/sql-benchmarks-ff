package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.FIOOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{NullFileFormat, SparkSession}

/**
  * Created by atr on 28.11.17.
  */
class DataGenerationTest (fioOptions:FIOOptions, spark:SparkSession) extends FIOTest {

  private val rdd = spark.range(0, fioOptions.getNumTasks)

  override def execute(): String = {
    rdd.foreach( longNumber => {

    })

  }

  override def explain(): Unit = ???

  override def plainExplain(): String = ???

  override def printAdditionalInformation(timelapsedinNanosec:Long): String ={

  }
}
