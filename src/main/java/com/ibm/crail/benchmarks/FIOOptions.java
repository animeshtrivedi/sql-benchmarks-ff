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

package com.ibm.crail.benchmarks;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by atr on 11.10.17.
 */
public class FIOOptions extends TestOptions {
    private Options options;
    private String inputLocations;
    private String warmUpinputLocations;
    private boolean withWarmup;
    private String test;
    private Map<String, String> inputFormatOptions; // input format options
    private Map<String, String> outputFormatOptions; // output format options
    private int parallelism; // spark parallelization
    private int numTasks;
    private long sizePerTask; // valid for writing
    private int align; // align to the block size
    private int requetSize;
    private boolean useFully;
    private int parquetAloneVersion;
    private String sparkFormat;

    public FIOOptions(){
        options = new Options();
        this.inputLocations = null;
        this.warmUpinputLocations = null;
        this.test = "HdfsRead";
        this.numTasks = 1;
        this.parallelism = 1;
        this.sizePerTask = 1024 * 1024; // 1MB
        this.align = 0; // alight to zero
        this.requetSize = 1024 * 1024; // 1MB
        this.withWarmup = false;
        this.inputFormatOptions = new HashMap<>(4);
        this.useFully = false;
        this.parquetAloneVersion = 3;
        this.sparkFormat = "parquet";

        options.addOption("h", "help", false, "show help.");
        options.addOption("i", "input", true, "[String] a location of input directory where files are read and written.");
        options.addOption("w", "warmupInput", true, "[String,...] a list of input files/directory used for warmup. Same semantics as the -i flag.");
        options.addOption("t", "test", true, "[String] which test to perform, HdfsRead, HdfsWrite, ParquetRead, ParquetWrite, SFFRead, SFFWrite, IteratorRead, SparkColumnarBatchRead, ParquetRowGroupTest, ParquetAloneTest, ORCAloneTest or SparkRead (see -sf). Default " + this.test);
        options.addOption("sf", "sparkFormatTest", true, "[String] which spark reading test to perform, ORC, parquet, json, avro, null, or sff. Default " + this.sparkFormat);
        options.addOption("ifo", "inputFormatOptions", true, "input format options as key0,value0,key1,value1...");
        options.addOption("so", "sparkOptions", true, "[<String,String>,...] options to set on SparkConf, NYI");
        options.addOption("n", "numTasks", true, "[Int] number of tasks");
        options.addOption("p", "parallel", true, "[Int] amoount of parallelism in terms of parallel spark tasks. Default: = numTasks");
        options.addOption("s", "size", true, "[Long] size per task. Takes prefixes like k, m, g, t");
        options.addOption("a", "align", true, "[Int] alignment");
        options.addOption("r", "requestSize", true, "[Int] request size ");
        options.addOption("f", "fully", false, " for HdfsRead test, use the readfully code.");
        options.addOption("pv", "parquetalone", true, "[int] parquet alone test version, 1 or 2. default is :" + this.parquetAloneVersion);
    }

    @Override
    public void parse(String[] args) {
        boolean parallismSet = false;
        if (args != null) {
            CommandLineParser parser = new GnuParser();
            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);

                if (cmd.hasOption("h")) {
                    show_help("SQL", this.options);
                    System.exit(0);
                }
                if (cmd.hasOption("i")) {
                    // get the value and split it
                    //this.inputLocations = cmd.getOptionValue("i").split(",");
                    this.inputLocations = cmd.getOptionValue("i");
                }
                if (cmd.hasOption("w")) {
                    // get the value and split it
                    //this.warmUpinputLocations = cmd.getOptionValue("w").split(",");
                    this.warmUpinputLocations = cmd.getOptionValue("w");
                    this.withWarmup = true;
                }
                if (cmd.hasOption("t")) {
                    this.test = cmd.getOptionValue("t").trim();
                }
                if (cmd.hasOption("sf")) {
                    this.sparkFormat = cmd.getOptionValue("sf").trim();
                }
                if (cmd.hasOption("ifo")) {
                    String[] one = cmd.getOptionValue("ifo").trim().split(",");
                    if (one.length % 2 != 0) {
                        errorAbort("Illegal format for inputFormatOptions. Number of parameters " + one.length + " are not even");
                    }
                    for (int i = 0; i < one.length; i += 2) {
                        this.inputFormatOptions.put(one[i].trim(), one[i + 1].trim());
                    }
                }
                if(cmd.hasOption("p")){
                    this.parallelism = Integer.parseInt(cmd.getOptionValue("p").trim());
                    parallismSet = true;
                }
                if(cmd.hasOption("n")){
                    this.numTasks = Integer.parseInt(cmd.getOptionValue("n").trim());
                    if(!parallismSet) {
                        /* if parallelism is not set explicitly then default it here */
                        this.parallelism = this.numTasks;
                    }
                }
                if(cmd.hasOption("a")){
                    this.align = Integer.parseInt(cmd.getOptionValue("a").trim());
                }
                if(cmd.hasOption("pv")){
                    this.parquetAloneVersion = Integer.parseInt(cmd.getOptionValue("pv").trim());
                    if(this.parquetAloneVersion < 0 || this.parquetAloneVersion > 2 ){
                        errorAbort(" valid parquet test numbers are 1 or 2, passed: " + this.parquetAloneVersion);
                    }
                }
                if(cmd.hasOption("r")){
                    this.requetSize = Integer.parseInt(cmd.getOptionValue("r").trim());
                }
                if(cmd.hasOption("s")){
                    this.sizePerTask = Utils.sizeStrToBytes2(cmd.getOptionValue("s").trim());
                }
                if(cmd.hasOption("f")){
                    this.useFully = true;
                }

            } catch (ParseException e) {
                errorAbort("Failed to parse command line properties" + e);
            }
        }
        if(!(isTestHdfsRead() || isTestHdfsWrite() || isTestPaquetRead() || isTestSFFRead() || isTestIteratorRead()
                || isTestSparkColumnarBatchReadTest() || isTestParquetRowGroupTest() || isTestParquetAloneTest()
                || isTestSparkReadTest() || isTestORCAloneTest())) {
            errorAbort("Illegal test name for FIO : " + this.test);
            if(isTestSparkReadTest()){
                if(!(isSRTParquet() || isSRTORC() || isSRTJson() || isSRTNull() || isSRTSFF() || isSRTAvro())){
                    errorAbort("Illegal test format name for Spark Reading : " + this.sparkFormat);
                }
            }
        }
        if(this.inputLocations == null && !(isTestIteratorRead() || (isTestSparkReadTest() && isSRTNull()))){
            // iterator read does not need a input file location
            errorAbort("Please specify input location with -i");
        }
    }

    @Override
    public void setWarmupConfig() {
        String temp = this.inputLocations;
        this.inputLocations = this.warmUpinputLocations;
        this.warmUpinputLocations = temp;
    }

    @Override
    public void restoreInputConfig() {
        String temp = this.inputLocations;
        this.inputLocations = this.warmUpinputLocations;
        this.warmUpinputLocations = temp;
    }

    @Override
    public boolean withWarmup() {
        return this.withWarmup;
    }


    public boolean isTestPaquetRead(){
        return this.test.compareToIgnoreCase("parquetread") == 0;
    }

    public boolean isTestSFFRead(){
        return this.test.compareToIgnoreCase("sffread") == 0;
    }

    public boolean isTestHdfsRead(){
        return this.test.compareToIgnoreCase("hdfsread") == 0;
    }

    public boolean isTestHdfsWrite(){
        return this.test.compareToIgnoreCase("hdfsWrite") == 0;
    }

    public boolean isTestIteratorRead(){
        return this.test.compareToIgnoreCase("iteratorRead") == 0;
    }

    public boolean isTestSparkColumnarBatchReadTest(){
        return this.test.compareToIgnoreCase("SparkColumnarBatchRead") == 0;
    }

    public boolean isTestParquetRowGroupTest(){
        return this.test.compareToIgnoreCase("ParquetRowGroupTest") == 0;
    }

    public boolean isTestParquetAloneTest(){
        return this.test.compareToIgnoreCase("ParquetAloneTest") == 0;
    }

    public boolean isTestORCAloneTest(){
        return this.test.compareToIgnoreCase("ORCAloneTest") == 0;
    }

    public boolean isTestSparkReadTest(){
        return this.test.compareToIgnoreCase("SparkRead") == 0;
    }

    public boolean isSRTParquet(){
        return this.sparkFormat.compareToIgnoreCase("parquet") == 0;
    }
    public boolean isSRTORC(){
        return this.sparkFormat.compareToIgnoreCase("orc") == 0;
    }
    public boolean isSRTJson(){
        return this.sparkFormat.compareToIgnoreCase("json") == 0;
    }
    public boolean isSRTSFF(){
        return this.sparkFormat.compareToIgnoreCase("sff") == 0;
    }
    public boolean isSRTNull(){
        return this.sparkFormat.compareToIgnoreCase("null") == 0;
    }
    public boolean isSRTAvro(){
        return this.sparkFormat.compareToIgnoreCase("avro") == 0;
    }

    public String getInputLocations(){
        return this.inputLocations;
    }

    public int getParallelism(){
        return this.parallelism;
    }

    public int getNumTasks(){
        return this.numTasks;
    }

    public long getSizePerTask(){
        return this.sizePerTask;
    }

    public int getAlign(){
        return this.align;
    }

    public int getRequetSize(){
        return this.requetSize;
    }

    public Map<String, String> getInputFormatOptions(){
        return this.inputFormatOptions;
    }

    public String getTestName(){
        return this.test;
    }

    public String getSparkFormat(){
        return this.sparkFormat;
    }

    public boolean isUseFully(){
        return this.useFully;
    }

    public int getParquetAloneVersion(){
        return this.parquetAloneVersion;
    }

}
