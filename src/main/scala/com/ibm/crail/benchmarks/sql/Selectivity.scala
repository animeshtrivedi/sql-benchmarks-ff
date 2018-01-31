package com.ibm.crail.benchmarks.sql

import com.ibm.crail.benchmarks.SQLOptions
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 31.01.18.
  */
class Selectivity (val sqlOptions: SQLOptions, spark:SparkSession) extends SQLTest(spark) {
  private val file = sqlOptions.getInputFiles()(0)
  println(" loading the file " + file)
  private val f1 = spark.read.format(sqlOptions.getInputFormat).options(sqlOptions.getInputFormatOptions).load(file)
  f1.printSchema()
  private val f2 = f1.where(f1("int0").leq(sqlOptions.getSelectivity))

  override def execute(): String = takeAction(sqlOptions, f2)

  override def explain(): Unit = f2.explain(true)

  override def plainExplain(): String = " selectivity on int0 column with LTE " + sqlOptions.getSelectivity
}
