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

package com.ibm.crail.benchmarks.sql.tpcds

import com.ibm.crail.benchmarks.SQLOptions
import com.ibm.crail.benchmarks.sql.SQLTest
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by atr on 04.09.17.
  */
class TPCDSTest (val sqlOptions: SQLOptions, spark:SparkSession) extends SQLTest(spark){
  // you set up the temp view
  TPCDSSetup.readAndRegisterTempTables(sqlOptions, spark)
  // we need 100 queries
  case class ResultWithQuery(df:DataFrame, queryName:String)
  private val selectedQueries = TPCDSQueries.queries.slice(sqlOptions.getStartIdx, sqlOptions.getEndIdx)

  private val result:Array[ResultWithQuery] = new Array[ResultWithQuery](selectedQueries.length)
  private var time:Array[Long] = new Array[Long](selectedQueries.length)
  private var i:Int = 0
  while (i <  selectedQueries.length) {
    val q = selectedQueries(i)
    result(i) = ResultWithQuery(spark.sql(q.query), q.id)
    i+=1
  }

  override def execute(): String = {
    var elapsedTime = 0L
    var i = 0
    while ( i < result.length) {
      if(result(i).queryName.compareToIgnoreCase("q72.sql") != 0) {
        val s = System.nanoTime()
        takeAction(sqlOptions, result(i).df, "/" + result(i).queryName)
        time(i) = System.nanoTime() - s
        elapsedTime += time(i)
        println((i + 1) + "/" + result.length + " executed query : " + result(i).queryName + " on " + sqlOptions.getInputFiles()(0) + " took " + time(i) / 1000000 + " msec | elapsedTime : " + elapsedTime / 1000000 + " msec ")
      } else {
        println((i + 1) + "/" + result.length + " SKIPPED query : " + result(i).queryName + " on " + sqlOptions.getInputFiles()(0) + " | elapsedTime : " + elapsedTime / 1000000 + " msec ")
      }
      i+=1
    }
    s"${sqlOptions.getAction.toString} for TPCDS"
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = s"TPC-DS on " + sqlOptions.getInputFiles()(0)

  override def printAdditionalInformation(timelapsedinNanosec:Long):String = {
    val sb = new StringBuilder
    i = 0
    while (i < selectedQueries.length){
      val q = selectedQueries(i)
      sb.append(" query " + q.id + " took : " + (time(i) / 1000000) + " msec\n")
      i+=1
    }
    sb.mkString
  }
}
