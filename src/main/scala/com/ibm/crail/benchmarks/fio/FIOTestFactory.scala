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
package com.ibm.crail.benchmarks.fio

import com.databricks.spark.avro.AvroSparkReadTest
import com.ibm.crail.benchmarks.{BaseTest, FIOOptions}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.NullioSparkReadTest
import org.apache.spark.sql.execution.datasources.json.JsonSparkReadTest
import org.apache.spark.sql.execution.datasources.parquet.ParquetSparkReadTest
import org.apache.spark.sql.hive.orc.ORCSparkReadTest

/**
  * Created by atr on 11.10.17.
  */
object FIOTestFactory {
  def getTestObject(fioOptions:FIOOptions, spark:SparkSession):BaseTest = {
    if(fioOptions.isTestHdfsWrite){
      new HdfsWriteTest(fioOptions, spark)
    } else if(fioOptions.isTestHdfsRead){
      new HdfsReadTest(fioOptions, spark)
    } else if (fioOptions.isTestPaquetRead){
      new ParquetReadTest(fioOptions, spark)
    } else if (fioOptions.isTestSFFRead) {
      new SFFReadTest(fioOptions, spark)
    } else if (fioOptions.isTestIteratorRead){
      new IteratorReadTest(fioOptions, spark)
    }  else if (fioOptions.isTestSparkColumnarBatchReadTest){
      new SparkColumnarBatchTest(fioOptions, spark)
    } else if (fioOptions.isTestParquetRowGroupTest){
      new ParquetRowGroupTest(fioOptions, spark)
    } else if (fioOptions.isTestParquetAloneTest){
      new ParquetAloneTest(fioOptions, spark)
    } else if (fioOptions.isTestSparkReadTest){
      if(fioOptions.isSRTParquet){
        new ParquetSparkReadTest(fioOptions, spark)
      } else if(fioOptions.isSRTORC) {
        new ORCSparkReadTest(fioOptions, spark)
      } else if (fioOptions.isSRTJson){
        new JsonSparkReadTest(fioOptions, spark)
      } else if (fioOptions.isSRTNull) {
        new NullioSparkReadTest(fioOptions, spark)
      } else if(fioOptions.isSRTSFF) {
        new SFFReadTest(fioOptions, spark)
      } else if(fioOptions.isSRTAvro) {
        new AvroSparkReadTest(fioOptions, spark)
      } else {
        throw new Exception("Illegal format name for spark reading ")
      }
    } else {
      throw new Exception("Illegal test name for FIO: + " + fioOptions.getTestName)
    }
  }
}
