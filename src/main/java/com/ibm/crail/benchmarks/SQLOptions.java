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

import com.ibm.crail.benchmarks.sql.Action;
import com.ibm.crail.benchmarks.sql.Collect;
import com.ibm.crail.benchmarks.sql.Count;
import com.ibm.crail.benchmarks.sql.Save;
import org.apache.commons.cli.*;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by atr on 11.10.17.
 *
 * This class is meant to encapsulate all SQL related options.
 */
public class SQLOptions extends TestOptions {
    private Options options;
    private String test;
    private String tpcdsQuery;
    private String[] inputFiles;
    private String[] warmupInputFiles;
    private String[] projection;
    private boolean doWarmup;
    private String joinKey;
    private Action action;
    private String inputFormat;
    private String outputFormat;
    private Map<String, String> inputFormatOptions;
    private Map<String, String> outputFormatOptions;
    private int startIdx, endIdx;
    private boolean verbose;
    private int selectivity;
    private int partitions;
    private int blockSize;
    private String compressionType;

    public SQLOptions(){
        options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", true, "which test to perform, options are (case insensitive): equiJoin, qXXX(tpcds queries), tpcds, readOnly, selectivity (on HundredCols) ");
        options.addOption("i", "input", true, "comma separated list of input files/directories. " +
                "EquiJoin takes two files, TPCDS queries takes a tpc-ds data directory, and readOnly take a file or a directory with files");
        options.addOption("w", "warmupInput", true, "warmup files with the same semantics as the -i option");
        options.addOption("k", "key", true, "key for EquiJoin, default is IntIndex");
        options.addOption("v", "verbose", false, "verbose");
        options.addOption("a", "action", true, "action to take. Your options are (important, no space between ','): \n" +
                " 1. count (default)\n" +
                " 2. collect,items[int, default: 100] \n" +
                " 3. save,filename[str, default: /tmp]");
        options.addOption("if", "inputFormat", true, "input format (where-ever applicable) default: parquet");
        options.addOption("ifo", "inputFormatOptions", true, "input format options as key0,value0,key1,value1...");
        options.addOption("of", "outputFormat", true, "output format (where-ever applicable) default: parquet");
        options.addOption("ofo", "outputFormatOptions", true, "output format options as key0,value0,key1,value1...");
        options.addOption("qr", "queryRange", true, "run tpcds queries from index x-y, vali ranges are (0-104)=>means all");
        options.addOption("S", "selectivity", true, "LTE selection for int0");

        // block size for any file format that supports it
        options.addOption("bs", "blockSize", true, "block size for any file format that supports it. At this time, Parquet and ORC");
        // number of output parition for a dataset, mostly useful when copying the data
        options.addOption("p", "partitions", true, "number of output parition for a dataset, mostly useful when copying the data");
        options.addOption("C", "compress", true, "<String> compression type for when writing out, valid values are: uncompressed, " +
                "snappy, gzip, lzo (default: "
                + this.compressionType+")");
        options.addOption("P", "projection", true, "Array[String] column names that will be read, default " + this.projection + " (null means no projection)");

        // set defaults
        this.joinKey = "intKey";
        this.inputFormat = "parquet";
        this.outputFormat = "parquet";
        this.verbose = false;
        this.action = new Count();
        this.doWarmup = false;
        this.tpcdsQuery = null;
        this.startIdx = 0;
        this.endIdx = 104;
        this.selectivity = 100;
        this.blockSize = -1;
        this.partitions = -1;
        this.compressionType = "uncompressed";
        this.projection = null;
        this.inputFormatOptions = new HashMap<>(4);
        this.outputFormatOptions = new HashMap<>(4);
    }

    @Override
    public void parse(String[] args) {
        if (args != null) {
            CommandLineParser parser = new GnuParser();
            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);

                if (cmd.hasOption("h")) {
                    show_help("SQL", this.options);
                    System.exit(0);
                }
                if (cmd.hasOption("t")) {
                    this.test = cmd.getOptionValue("t").trim();
                    if (this.test.charAt(0) == 'q') {
                    /* specific query test */
                        this.tpcdsQuery = this.test;
                    }
                }
                if (cmd.hasOption("v")) {
                    this.verbose = true;
                }
                if (cmd.hasOption("k")) {
                    this.joinKey = cmd.getOptionValue("k").trim();
                }
                if (cmd.hasOption("if")) {
                    this.inputFormat = cmd.getOptionValue("if").trim();
                    if (this.inputFormat.compareToIgnoreCase("nullio") == 0) {
                        this.inputFormat = "org.apache.spark.sql.NullFileFormat";
                    }
                }
                if (cmd.hasOption("of")) {
                    this.outputFormat = cmd.getOptionValue("of").trim();
                    if (this.outputFormat.compareToIgnoreCase("nullio") == 0) {
                        this.outputFormat = "org.apache.spark.sql.NullFileFormat";
                    }
                }
                if (cmd.hasOption("ofo")) {
                    String[] one = cmd.getOptionValue("ofo").trim().split(",");
                    if (one.length % 2 != 0) {
                        errorAbort("Illegal format for outputFormatOptions. Number of parameters " + one.length + " are not even");
                    }
                    for (int i = 0; i < one.length; i += 2) {
                        this.outputFormatOptions.put(one[i].trim(), one[i + 1].trim());
                    }
                }

                if (cmd.hasOption("qr")) {
                    String[] one = cmd.getOptionValue("qr").trim().split(",");
                    if (one.length != 2) {
                        errorAbort("Illegal format for queryRange, number of parameters " + one.length + " are not even. We expect 2 integers x,y");
                    }
                    this.startIdx = Integer.parseInt(one[0]);
                    this.endIdx = Integer.parseInt(one[1]);
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

                if (cmd.hasOption("S")) {
                    this.selectivity = Integer.parseInt(cmd.getOptionValue("S").trim());
                }

                if (cmd.hasOption("bs")) {
                    this.blockSize = Integer.parseInt(cmd.getOptionValue("bs").trim());
                }

                if (cmd.hasOption("p")) {
                    this.partitions = Integer.parseInt(cmd.getOptionValue("p").trim());
                }

                if (cmd.hasOption("i")) {
                    // get the value and split it
                    this.inputFiles = cmd.getOptionValue("i").split(",");
                    for (String inputFile : this.inputFiles) {
                        inputFile.trim();
                    }
                }

                if (cmd.hasOption("P")) {
                    // get the value and split it
                    this.projection = cmd.getOptionValue("P").split(",");
                    for (String colName : this.projection) {
                        colName.trim();
                    }
                }
                if (cmd.hasOption("w")) {
                    // get the value and split it
                    this.warmupInputFiles = cmd.getOptionValue("w").split(",");
                    for (String inputFile : this.warmupInputFiles) {
                        inputFile.trim();
                    }
                    this.doWarmup = true;
                }
                if (cmd.hasOption("a")) {
                    String[] tokens = cmd.getOptionValue("a").split(",");
                    if (tokens.length == 0) {
                        errorAbort("Failed to parse command line properties " + cmd.getOptionValue("a"));
                    }
                    if (tokens[0].compareToIgnoreCase("count") == 0) {
                        this.action = new Count();
                    } else if (tokens[0].compareToIgnoreCase("collect") == 0) {
                        int items = 0;
                        if (tokens.length != 2) {
                            items = 100;
                        } else {
                            items = Integer.parseInt(tokens[1].trim());
                        }
                        this.action = new Collect(items);
                    } else if (tokens[0].compareToIgnoreCase("save") == 0) {
                        String fileName = (tokens.length >= 2) ? tokens[1].trim() : "/sql-benchmark-output";
                        this.action = new Save(fileName);
                    } else {
                        errorAbort("ERROR: illegal action name : " + tokens[0]);
                    }
                }

            } catch (ParseException e) {
                errorAbort("Failed to parse command line properties" + e);
            }
            // if not files are set
            if (this.inputFiles == null) {
                errorAbort("ERROR:" + " please specify some input files for the SQL test");
            }
            // check valid test names
            if (!isTestEquiJoin()
                    && !isTestQuery()
                    && !isTestTPCDS()
                    && !isTestReadOnly()
                    && !isTestSelectivity()
                    && !isTestCopy()) {
                errorAbort("ERROR: illegal test name : " + this.test);
            }
        /* some sanity checks */
            if (isTestEquiJoin() && this.inputFiles.length != 2) {
                errorAbort("ERROR:" + this.test + " needs two files as inputs");
            }

            if(isTestCopy() && !(this.action instanceof Save)){
                errorAbort("ERROR:" + this.test + " only takes save action.");
            }
        }

        // process the compression and block size options for ORC
        if(this.outputFormat.compareToIgnoreCase("orc") == 0){
            // compression settings
            if(this.compressionType.compareToIgnoreCase("uncompressed") == 0 ||
                    this.compressionType.compareToIgnoreCase("none") == 0) {
                    this.outputFormatOptions.put("orc.compress","none");
            } else {
                this.outputFormatOptions.put("orc.compress",this.compressionType);
            }

            if(this.blockSize != -1){
                String size = Integer.toString(this.blockSize);
                // set the block size
                this.outputFormatOptions.put("orc.stripe.size", size);
            }
        }

        // for parquet specific settings
        if(this.outputFormat.compareToIgnoreCase("parquet") == 0){
            // compression settings are set on the spark configuration !
            // we just check the block size
            if(this.blockSize != -1){
                String size = Integer.toString(this.blockSize);
                // set the block size
                this.outputFormatOptions.put("parquet.block.size", size);
            }
        }
    }

    @Override
    public void setWarmupConfig() {
     String[] temp = this.inputFiles;
     this.inputFiles = this.warmupInputFiles;
     this.warmupInputFiles = temp;
    }

    @Override
    public void restoreInputConfig() {
        String[] temp = this.inputFiles;
        this.inputFiles = this.warmupInputFiles;
        this.warmupInputFiles = temp;
    }

    @Override
    public boolean withWarmup() {
        return this.doWarmup;
    }

    @Override
    public String getTestName() {
        return this.test;
    }

    public boolean isTestCopy(){
        return this.test.compareToIgnoreCase("Copy") == 0;
    }

    public boolean isTestSelectivity(){
        return this.test.compareToIgnoreCase("Selectivity") == 0;
    }

    public boolean isTestEquiJoin(){
        return this.test.compareToIgnoreCase("EquiJoin") == 0;
    }

    public boolean isTestQuery(){
        return !(this.tpcdsQuery == null);
    }

    public boolean isTestTPCDS(){
        return (this.test.compareToIgnoreCase("tpcds") == 0);
    }

    public boolean isTestReadOnly(){
        return this.test.compareToIgnoreCase("readOnly") == 0;
    }

    public String[] getInputFiles(){
        return this.inputFiles;
    }

    public String getJoinKey() {
        return this.joinKey;
    }

    public Action getAction(){
        return this.action;
    }

    public boolean getVerbose(){
        return this.verbose;
    }

    public String getInputFormat(){
        return this.inputFormat;
    }

    public String getOutputFormat(){
        return this.outputFormat;
    }

    public Map<String, String> getInputFormatOptions(){
        return this.inputFormatOptions;
    }

    public Map<String, String> getOutputFormatOptions(){
        return this.outputFormatOptions;
    }

    public String getTPCDSQuery(){
        return this.tpcdsQuery;
    }

    public int getStartIdx(){
        return this.startIdx;
    }

    public int getEndIdx(){
        return this.endIdx;
    }

    public int getSelectivity(){
        return this.selectivity;
    }

    public int getPartitions(){
        return this.partitions;
    }

    public void setSparkSpecificSettings(SparkSession session){
        // parquet needs it on the spark session
        if(this.outputFormat.compareToIgnoreCase("parquet") == 0) {
            session.sqlContext().setConf("spark.sql.parquet.compression.codec", this.compressionType);
        }
    }
}
