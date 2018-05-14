package com.ibm.crail.benchmarks.sql

import com.ibm.crail.benchmarks.SQLOptions
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 31.01.18.
  */
class Copy (val sqlOptions: SQLOptions, spark:SparkSession) extends SQLTest(spark) {
  private val file = sqlOptions.getInputFiles()(0)
  private val ds = spark.read.format(sqlOptions.getInputFormat).options(sqlOptions.getInputFormatOptions).load(file)
  // now we need to transform it and save it again
  val parts = sqlOptions.getPartitions
  val outputDs = if(parts == -1) {
    ds
  } else {
    ds.repartition(parts)
  }

  override def execute(): String = takeAction(sqlOptions, outputDs)

  override def explain(): Unit = outputDs.explain(true)

  override def plainExplain(): String = "Copying data from " + file + " in " + sqlOptions.getInputFormat + " to " + sqlOptions.getOutputFormat
}
