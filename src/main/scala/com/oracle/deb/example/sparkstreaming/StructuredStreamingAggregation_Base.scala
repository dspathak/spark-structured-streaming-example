package com.oracle.deb.example.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import java.sql.Timestamp
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.time.ZoneId

import Utilities._

case class EventBean(id:String, command:String, recordType:String, miscInfo:String, category:String, cloudService:String, cloudProvider:String, eventCount:Int, dataBytes:Int, time:Timestamp)

abstract class StructuredStreamingAggregation_Base {
  
   val eventSchema = StructType(Array(
       StructField("id", StringType),
       StructField("command", StringType),
       StructField("recordType", StringType),
       StructField("miscInfo", StringType),
       StructField("category", StringType),
       StructField("cloudService", StringType),
       StructField("cloudProvider", StringType),
       StructField("eventCount", IntegerType),
       StructField("dataBytes", IntegerType),
       StructField("time", TimestampType)
      ))
   
   def runStreamingQueriesFromFileInput(sparkSession:SparkSession, INPUT_DIRECTORY:String, BROKERLIST:String, TOPIC:String, TIMEWINDOW:String, WATERMARK:String) {
     
      setupLogging()
      
      val df = sparkSession
        .readStream
        .schema(eventSchema)
        .json(INPUT_DIRECTORY)
        //.as(Encoders.bean(EventBean.class))
        
      runStreamingQueries(sparkSession, df, BROKERLIST, TOPIC, TIMEWINDOW, WATERMARK)
   }
   
   def runStreamingQueriesFromKafkaInput(sparkSession:SparkSession, INPUT_DIRECTORY:String, BROKERLIST:String, TOPIC:String, TIMEWINDOW:String, WATERMARK:String) {
     
      setupLogging()
      
      val df = sparkSession
        .readStream
        .schema(eventSchema)
        .json(INPUT_DIRECTORY)
        //.as(Encoders.bean(EventBean.class))
        
      runStreamingQueries(sparkSession, df, BROKERLIST, TOPIC, TIMEWINDOW, WATERMARK)
   }
   
   def runStreamingQueries(sparkSession:SparkSession, df:DataFrame, BROKERLIST:String, TOPIC:String, TIMEWINDOW:String, WATERMARK:String) {
     
      import sparkSession.implicits._
      
      val ds = df.map {case Row(id:String,command:String,recordType:String,miscInfo:String,category:String,cloudService:String,cloudProvider:String,eventCount:Int,dataBytes:Int,time:Timestamp)
                            => EventBean(id,command,recordType,miscInfo,category,cloudService,cloudProvider,eventCount,dataBytes,convertToUTC(time))}
      
      val dsw = ds.withWatermark("time", WATERMARK)
      
      val toString = udf{(window:GenericRowWithSchema) => window.mkString("-")}
      val toStringStartTime = udf{(window:GenericRowWithSchema) => window.mkString("-").substring(0, 21).replace(' ', 'T').concat("00Z")}
      
      // ***GROUPBY with a tumbling window.      
      val windowed0 = dsw.groupBy(window($"time", TIMEWINDOW, TIMEWINDOW).as("time")).agg(sum("eventCount").as("eventCount"), sum("dataBytes").as("dataBytes"), approx_count_distinct("cloudService").as("approxservicecount"), approx_count_distinct("cloudProvider").as("approxprovidercount"))      
      val windowed0_1 = windowed0.withColumn("id", lit("XYZ")).withColumn("command", lit(TIMEWINDOW)).withColumn("recordType", lit("b_aggr")).withColumn("miscInfo", lit("all_counts")).withColumn("cloudService", lit("NA")).withColumn("cloudProvider", lit("NA")).withColumn("category", lit("cloud"))
      val windowed0_2 = windowed0_1.select(to_json(struct($"id",$"command",$"recordType",$"miscInfo",$"cloudService",$"approxservicecount",$"cloudProvider",$"approxprovidercount",$"category",toStringStartTime($"time").as("time"))).as("value"))
      
      val windowed1 = dsw.groupBy(window($"time", TIMEWINDOW, TIMEWINDOW).as("time"), $"cloudService".as("cloudService")).agg(sum("eventCount").as("eventCount"), sum("dataBytes").as("dataBytes"))//count()//.orderBy("window")
      val windowed1_1 = windowed1.withColumn("id", lit("XYZ")).withColumn("command", lit(TIMEWINDOW)).withColumn("recordType", lit("b_aggr")).withColumn("miscInfo", lit("service_counts")).withColumn("cloudProvider", lit("NA")).withColumn("category", lit("cloud"))
      val windowed1_2 = windowed1_1.select(to_json(struct($"id",$"command",$"recordType",$"miscInfo",$"cloudService",$"cloudProvider",$"category",$"eventCount",$"dataBytes",toStringStartTime($"time").as("time"))).as("value"))
                  
      val windowed2 = dsw.groupBy(window($"time", TIMEWINDOW, TIMEWINDOW).as("time"), $"cloudProvider".as("cloudProvider")).agg(sum("eventCount").as("eventCount"), sum("dataBytes").as("dataBytes"))
      val windowed2_1 = windowed2.withColumn("id", lit("XYZ")).withColumn("command", lit(TIMEWINDOW)).withColumn("recordType", lit("b_aggr")).withColumn("miscInfo", lit("provider_counts")).withColumn("cloudService", lit("NA")).withColumn("category", lit("cloud"))
      val windowed2_2 = windowed2_1.select(to_json(struct($"id",$"command",$"recordType",$"miscInfo",$"cloudService",$"cloudProvider",$"category",$"eventCount",$"dataBytes",toStringStartTime($"time").as("time"))).as("value"))
      
      // ***START the streaming query
      //val query = windowed.writeStream.outputMode("update").format("console").start()
      //val query = windowed.writeStream.trigger(ProcessingTime("1 minutes")).outputMode("append").format("json").option("path", OUTPUT_DIRECTORY).start()   
      
      val query0 = windowed0.writeStream.trigger(ProcessingTime("1 minutes")).outputMode("update").format("console").start()
      //val query0 = windowed0_2.selectExpr("CAST(value AS STRING)").writeStream.trigger(ProcessingTime("1 minutes")).outputMode("update").format("kafka").option("kafka.bootstrap.servers", BROKERLIST).option("topic", TOPIC).start()
      
      val query1 = windowed1.writeStream.trigger(ProcessingTime("1 minutes")).outputMode("update").format("console").start()
      //val query1 = windowed1_2.selectExpr("CAST(value AS STRING)").writeStream.trigger(ProcessingTime("1 minutes")).outputMode("update").format("kafka").option("kafka.bootstrap.servers", BROKERLIST).option("topic", TOPIC).start()
      
      val query2 = windowed2.writeStream.trigger(ProcessingTime("1 minutes")).outputMode("update").format("console").start()
      //val query2 = windowed2_2.selectExpr("CAST(value AS STRING)").writeStream.trigger(ProcessingTime("1 minutes")).outputMode("update").format("kafka").option("kafka.bootstrap.servers", BROKERLIST).option("topic", TOPIC).start()
       
      // ***Keep going until we're stopped.
      query0.awaitTermination()
      query1.awaitTermination()
      query2.awaitTermination()
      
      sparkSession.stop()
   }
}

