package weblog

import java.sql.Timestamp

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory

/**
  * Created by Kai on 2016-09-11.
  */
object WebLogChallengeDF extends App {

  val Log = Logger(LoggerFactory.getLogger(WebLogChallengeDF.getClass))

  System.setProperty("hadoop.home.dir", "E:/Debug/IntelliJ/WebLogChallenge/")
  val sparkSession = SparkSession
    .builder
    .appName(this.getClass.getName)
    .master("local")
    .config("spark.sql.warehouse.dir", "file:///E:/Debug/IntelliJ/WebLogChallenge/")
    .getOrCreate

  import sparkSession.implicits._

  val logFileData = sparkSession.sparkContext.textFile("2015_07_22_mktplace_shop_web_log_sample.log", 6)
  Log.info(s"File Data Set Count: ${logFileData.count}")

  // define web log schema
  val webLogFieldTupleList = List(("timestamp", TimestampType), ("elb", StringType)
    , ("clientIP", StringType), ("clientPort", StringType), ("backendIP", StringType), ("backendPort", StringType)
    , ("requestProcessingTime", DoubleType), ("backendProcessingTime", DoubleType), ("responseProcessingTime", DoubleType)
    , ("elbStatusCode", StringType), ("backendStatusCode", StringType)
    , ("receivedBytes", IntegerType), ("sentBytes", IntegerType)
    , ("httpMethod", StringType), ("url", StringType), ("httpVersion", StringType), ("userAgent", StringType)
    , ("sslCipher", StringType), ("sslProtocl", StringType))
  val structFieldList = webLogFieldTupleList.map(webLogFieldsTuple => StructField(webLogFieldsTuple._1, webLogFieldsTuple._2))
  val webLogSchema = StructType(structFieldList)

  import weblog.WebLogHelper._

  //map log file to web log RDD
  val webLogRDD = logFileData
    .map(toWebLogTuple)
    .filter(_.isDefined)
    .map(_.get)
    .map(Row.fromTuple)

  // apply web log schema to web log RDD to get data frame
  val webLogDF = sparkSession.createDataFrame(webLogRDD, webLogSchema).cache
  webLogDF.printSchema()
  webLogDF.createOrReplaceTempView("webLog")

  //group by client IP, aggregate min of timestamp, total hists pages for each IP logs
  val webLogByIP = webLogDF.groupBy($"clientIP")
  val webLogByIPAgg = webLogByIP
    .agg(min($"timestamp").as("min_timestamp"), count($"url").as("total_hits_pages"))
  val webLogByIPWithAgg = webLogDF.as("wld")
    .join(webLogByIPAgg.as("wlamt"))
    .where($"wld.clientIP" === $"wlamt.clientIP")

  //session index is number of session windows between min timestamp in a webLogSet grouped by IP and a webLog timestamp in the webLogSet
  val sessionIndex = (minDate: Timestamp, date: Timestamp) => {
    Seconds.secondsBetween(new DateTime(minDate.getTime), new DateTime(minDate.getTime)).getSeconds / SESSION_WINDOW_TIME_IN_SECOND toInt
  }
  val sessionIndexUDF = udf(sessionIndex)
  //then group by session for each WebLogSet grouped by IP
  val webLogByIPSession =  webLogByIPWithAgg
    .withColumn("sessionIndex", sessionIndexUDF($"min_timestamp", $"timestamp"))
    .groupBy($"wlamt.clientIP", $"sessionIndex")

  val duration = (timestamp1: Timestamp, timestamp2: Timestamp) => {
    Seconds.secondsBetween(new DateTime(timestamp1.getTime), new DateTime(timestamp2.getTime)).getSeconds
  }
  val durationUDF = udf(duration)

  //unique URL visits
  val webLogByIPSessionAgg = webLogByIPSession.agg(
    countDistinct($"url").as("unique_url_visits_by_session"),
    min($"timestamp").as("min_session_timestamp"),
    max($"timestamp").as("max_session_timestamp"))
    .withColumn("session_duration", durationUDF($"min_session_timestamp", $"max_session_timestamp"))
  val avgSessionTime = (totalSessionTime: Double, session_count: Double) => totalSessionTime / session_count
  val avgSessionTimeUDF = udf(avgSessionTime)

  //the average session time
  val avgSessionTimeAgg = webLogByIPSessionAgg
    .groupBy($"clientIP")
    .agg(count($"sessionIndex").as("session_count"),
      sum($"session_duration").as("total_session_time"),
      max($"session_duration").as("max_session_time"))
      .withColumn("avg_session_time", avgSessionTimeUDF($"total_session_time", $"session_count"))

  //the IPs with the longest total session time by IP
  val totalVisitTimeByIPAgg = avgSessionTimeAgg
    .groupBy($"clientIP")
    .agg(sum("total_session_time").as("total_visit_time"))
  val maxTotalVisitTime = totalVisitTimeByIPAgg.agg(max("total_visit_time").as("max")).rdd.first.getLong(0)
  val ipsWithLongestTotalSessionTimeByIP = totalVisitTimeByIPAgg.where($"total_visit_time" === maxTotalVisitTime)

  //the IPs with the longest session time by session
  val maxTotalSessionTime = avgSessionTimeAgg.agg(max("total_session_time").as("max")).rdd.first.getLong(0)
  val ipsWithLongestSessionTimeBySession = avgSessionTimeAgg.where($"total_session_time" === maxTotalSessionTime).distinct

}
