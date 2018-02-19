package com.oracle.deb.example.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object SparkStructuredStreamingSMAClusterAggregationV3 extends StructuredStreamingAggregation_Base {
  
   val INPUT_DIRECTORY = "hdfs://adc01dys.us.oracle.com:9000/user/dspathak/sparkdata/staging";
   //val INPUT_DIRECTORY = "/work/projects/expt/data/staging";
   //val OUTPUT_DIRECTORY = "/work/projects/expt/data/output";
   //val BROKERLIST = "localhost:9092"
   val BROKERLIST = "adc01dys.us.oracle.com:9092"
   val TOPIC = "structuredstreaming_aggregation_topic"
   val TIMEWINDOW = "3 minutes"
   val WATERMARK = "1 minute"
   
   def main(args: Array[String]) {
     
      // This will be run via the spark-submit command either on local or a remote cluster
     
      /*
      val spark = SparkSession
        .builder
        .appName("StructuredStreaming")
        .config("spark.sql.warehouse.dir", "file:///C:/temp2") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint2")
        .getOrCreate() 
      */
     
      val spark = SparkSession
        .builder
        .appName("StructuredStreaming")
        .config("spark.sql.streaming.checkpointLocation", "hdfs://adc01dys.us.oracle.com:9000/user/dspathak/checkpoint2/")
        .getOrCreate()
     
      runStreamingQueriesFromFileInput(spark, INPUT_DIRECTORY, BROKERLIST, TOPIC, TIMEWINDOW, WATERMARK)
   }
}

