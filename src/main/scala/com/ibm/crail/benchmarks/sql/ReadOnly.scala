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

package com.ibm.crail.benchmarks.sql

import com.ibm.crail.benchmarks.SQLOptions
import org.apache.spark.sql.{Dataset, SparkSession, _}

/**
  * Created by atr on 05.05.17.
  */
class ReadOnly(val sqlOptions: SQLOptions, spark:SparkSession) extends SQLTest(spark) {
  private val fmt = sqlOptions.getInputFormat
  private val inputfiles = sqlOptions.getInputFiles
  private val columns:Array[Column] = makeColumns(sqlOptions)
  //println("Number of input files are : " + inputfiles.length + " with format " + fmt + " columns: " + sqlOptions.processSelectedColumns)

  private val readDataSetArrAll:Array[Dataset[Row]] = new Array[Dataset[Row]](inputfiles.length)
  var f = 0
  // step 1: we first read all of them
  while ( f < inputfiles.length ){
    readDataSetArrAll(f) = spark.read.format(fmt).options(sqlOptions.getInputFormatOptions).load(inputfiles(f))
    f+=1
  }

  // step 2: now we put the projection
  private val readDataSetArrProjected:Array[Dataset[Row]] = new Array[Dataset[Row]](inputfiles.length)
  f = 0
  while (f < inputfiles.length){
    readDataSetArrProjected(f) = if(columns != null) {
      readDataSetArrAll(f).select(columns: _*)
    } else {
      readDataSetArrAll(f)
    }
    f+=1
  }

  // step 3: we then make a union of them
  var finalDataset:Dataset[Row] = readDataSetArrProjected(0) // assign the first one
  // re-use "i"
  f = 1 // start the counter from 1
  while (f < readDataSetArrProjected.length){
    finalDataset = readDataSetArrProjected(f).union(finalDataset)
    f+=1
  }

  private def makeColumns(sqlOptions:SQLOptions):Array[Column] = {
    val projection = sqlOptions.getProjection
    if(projection != null){
      var col = 0
      val result:Array[Column] = new Array[Column](projection.length)
      while( col < projection.length){
        // for every name, we create an annoymous column with the column name
        result(col) = new Column(projection(col))
        col+=1
      }
      result
    } else {
      null
    }
  }

  // we do cache here, because now any action will trigger the whole data set reading
  // even the count().
  override def execute(): String = {
    takeAction(sqlOptions, finalDataset)
  }

  // TODO: this needs to be investigated, the performance difference
  //  override def execute(): String = {
  //    takeActionArray(options, readDataSetArr)
  //  }

  override def explain(): Unit = finalDataset.explain(true)

  override def plainExplain(): String = {
    var str:String = ""
    inputfiles.foreach(f=> str+=f+",")
    "ReadOnly (with .cache()) test on " + inputfiles.length + " files as: " + str + " projected columns: " + sqlOptions.processSelectedColumns
  }

  override def printAdditionalInformation(timelapsedinNanosec:Long):String = {
    if(sqlOptions.getProjection != null){
      "original schema  : " + (readDataSetArrAll(0).schema) + "\n" +
      "projected schema : " + (readDataSetArrProjected(0).schema)+ "\n"
    } else {
      "schema: " + finalDataset.schema + "\n"
    }
  }
}
