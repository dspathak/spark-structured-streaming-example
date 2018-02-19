// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os

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

import java.sql.Timestamp
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.time.ZoneId

import Utilities._

object StructuredStreamingAggregation_FromKafka2Kafka {
  
   case class OsefEvent(id:String, sefCommand:String, sefRecordType:String, sefMiscInfo:String, sefDestinationEPClassCategory:String, sefDestinationEPClassService:String, sefDestinationEPServiceProvider:String, sefContributorCount:Int, sefUnitsTransferred:Int, time:Timestamp)
   
   val osefSchema = StructType(Array(
       StructField("id", StringType),
       StructField("sefCommand", StringType),
       StructField("sefRecordType", StringType),
       StructField("sefMiscInfo", StringType),
       StructField("sefDestinationEPClassCategory", StringType),
       StructField("sefDestinationEPClassService", StringType),
       StructField("sefDestinationEPServiceProvider", StringType),
       StructField("sefContributorCount", IntegerType),
       StructField("sefUnitsTransferred", IntegerType),
       StructField("time", TimestampType)
      ))
   
   def main(args: Array[String]) {
     
      val spark = SparkSession
        .builder
        .appName("StructuredStreaming")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp1") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint1")
        .getOrCreate()
        
      setupLogging()
      
      import spark.implicits._
      
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "osef_events")
        .load()
      
      //val ds: Dataset[(String, String)] =df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]      
      val df1 = df.selectExpr("CAST(value AS STRING) as json").select(from_json($"json", osefSchema).as("osef")).select($"osef.id",
                                                                                                                        $"osef.sefCommand",
                                                                                                                        $"osef.sefRecordType",
                                                                                                                        $"osef.sefMiscInfo",
                                                                                                                        $"osef.sefDestinationEPClassCategory",
                                                                                                                        $"osef.sefDestinationEPClassService",
                                                                                                                        $"osef.sefDestinationEPServiceProvider",
                                                                                                                        $"osef.sefContributorCount",
                                                                                                                        $"osef.sefUnitsTransferred",
                                                                                                                        $"osef.time") 
                                                                                                                        //unix_timestamp($"osef.time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))    
      val ds = df1.map { 
        case Row(id:String,
                 sefCommand:String,
                 sefRecordType:String,
                 sefMiscInfo:String,
                 sefDestinationEPClassCategory:String,
                 sefDestinationEPClassService:String,
                 sefDestinationEPServiceProvider:String,
                 sefContributorCount:Int,
                 sefUnitsTransferred:Int,
                 time:Timestamp) => OsefEvent(id,
                                           sefCommand,
                                           sefRecordType,
                                           sefMiscInfo,
                                           sefDestinationEPClassCategory,
                                           sefDestinationEPClassService,
                                           sefDestinationEPServiceProvider,
                                           sefContributorCount,
                                           sefUnitsTransferred,
                                           time)
      }
      
      //.select(from_json($"json", osefSchema).as("osef")).as[OsefEvent]
      
      
//      val ds: Dataset[String] =df.selectExpr("CAST(value AS STRING) as json").select(from_json($"json", osefSchema).as("osef").as[String]      
//      val df = ds1.selectExpr("cast (value as string) as json").select(from_json($"json", schema=schema).as("data")).select("data.payload")      
//      .select(from_json($"value", osefSchema).as("osef"), $"timestamp")
      
      
//      from_json($"value", mySchema).as("data")
//      to_json(struct(*)) AS value
      
//      val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
//                  .select(from_json($"value", mySchema).as("data"), $"timestamp")
//                  .select("data.*", "timestamp")

      //df.printSchema()      
      
      /*
      // Create a stream of text files dumped into the logs directory
      val rawData = spark.readStream.text("logs")       
            
      // Convert our raw text into a DataSet of LogEntry rows, then just select the two columns we care about
      val structuredData = rawData.flatMap(parseLog).select("status", "dateTime")
      * 
      */
    
      // Group by status code, with a one-hour window.
      //val windowed = ds.groupBy(window($"time", "5 minutes", "5 minutes"), $"sefDestinationEPClassService").count()//.orderBy("window")
      val windowed = ds.withWatermark("time", "1 minute").groupBy(window($"time", "5 minutes", "5 minutes"), $"sefDestinationEPClassService").count()//.orderBy("window")
      
      
      
      // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating
      // (instead of "append").
      //val query = windowed.writeStream.outputMode("update").format("console").start()
      val query = windowed.writeStream.trigger(ProcessingTime("5 minutes")).outputMode("append").format("console").start()
      
      
//      val query: StreamingQuery = ds.writeStream
//        .outputMode("append")
//        .format("console")
//        .start()
      
      // Keep going until we're stopped.
      query.awaitTermination()
      
      spark.stop()
   }
}

