package com.deb.example.sparkstreaming

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.sql.Timestamp


object Utilities {
	/** Makes sure only ERROR messages get logged to avoid log spam. */
	def setupLogging() = {
			import org.apache.log4j.{Level, Logger}   
			val rootLogger = Logger.getRootLogger()
			rootLogger.setLevel(Level.ERROR)   
	}

	/** Configures Twitter service credentials using twiter.txt in the main workspace directory */
	def setupTwitter() = {
			import scala.io.Source

			for (line <- Source.fromFile("../twitter.txt").getLines) {
				val fields = line.split(" ")
				if (fields.length == 2) {
					System.setProperty("twitter4j.oauth." + fields(0), fields(1))
				}
			}
	}

	/** Retrieves a regex Pattern for parsing Apache access logs. */
	def apacheLogPattern():Pattern = {
			val ddd = "\\d{1,3}"                      
			val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
			val client = "(\\S+)"                     
			val user = "(\\S+)"
			val dateTime = "(\\[.+?\\])"              
			val request = "\"(.*?)\""                 
			val status = "(\\d{3})"
			val bytes = "(\\S+)"                     
			val referer = "\"(.*?)\""
			val agent = "\"(.*?)\""
			val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
			Pattern.compile(regex)    
	}

	/** Extracts the value of a field in a json given the field name. */
	def extractJsonSingleFieldValueUsingStringIndexes(fieldName:String, jsonLine:String):String = {
			var fieldValue:String = "NotSet";
	    val i = jsonLine.indexOf(fieldName);
	    val j = jsonLine.indexOf(':', i);
	    val beginIndex = jsonLine.indexOf('"', j);
    	val endIndex = jsonLine.indexOf('"', beginIndex+1);
	    fieldValue = (jsonLine.substring(beginIndex+1, endIndex)).trim();
	    return fieldValue;
	}
	
	def extractJsonSingleFieldNumericValueUsingStringIndexes(fieldName:String, jsonLine:String):Long = {
			val i = jsonLine.indexOf(fieldName);
	    val j = jsonLine.indexOf(':', i);
	    val beginIndex = j;
    	val endIndex = jsonLine.indexOf(',', beginIndex+1);
	    val fieldValue = (jsonLine.substring(beginIndex+1, endIndex)).trim().toLong
	    return fieldValue;
	}
	
	def returnFormattedTime(ts: Long): String = {
    val date = Instant.ofEpochMilli(ts)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())
    val formattedDate = formatter.format(date)
    formattedDate
  }
	
	def parseDateString1(text: String ): Long = {
	  //2017-08-15T19:30:00Z
    //SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'",Locale.US);
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss'Z'").withZone(ZoneId.of("UTC"))
//    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
//    return dateFormat.parse(text).getTime();
    return ZonedDateTime.parse(text,formatter).toEpochSecond()    
  }
	
	def convertToUTC(ts: Timestamp): Timestamp = {
	  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("UTC"))
    Timestamp.valueOf(ZonedDateTime.ofInstant(ts.toInstant(), ZoneOffset.UTC).toLocalDateTime())
	}
	
	def stripTimeZone(time: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
	  //time.substring(0, time.indexOf('+')).concat("Z")
	  null
	}

}