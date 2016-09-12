package weblog

import com.typesafe.scalalogging.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Seconds}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashSet

/**
  * Created by kai.zhu on 2016-09-06.
  */
object WebLogChallenge extends App {

  val Log = Logger(LoggerFactory.getLogger(WebLogChallenge.getClass))

  System.setProperty("hadoop.home.dir", "E:/Debug/IntelliJ/WebLogChallenge/")
  val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sparkContext = new SparkContext(sparkConf)

  val logFileData = sparkContext.textFile("E:/Debug/IntelliJ/WebLogChallenge/2015_07_22_mktplace_shop_web_log_sample.log", 6)
  Log.info(s"File Data Count: ${logFileData.count}")

  import weblog.WebLogHelper._

  //parse log into WebLog object
  val webLogRDD = logFileData.mapPartitions(_.map(toWebLog)).filter(_.isDefined).map(_.get).cache
  Log.info(s"Web Log RDD Count: ${webLogRDD.count}")

  val emptyWebLogdataset = HashSet.empty[WebLog]
  val addTodataset = (dataset: HashSet[WebLog], webLog: WebLog) => dataset + webLog
  val mergePartitionSets = (partition1: HashSet[WebLog], partition2: HashSet[WebLog]) => partition1 ++ partition2

  //map to (ip, webLog) tuple
  val ipWebLogTuple = webLogRDD.map(webLog => (webLog.client_port._1, webLog))
  //group by client IP (IP, WebLogSet)
  val webLogByIP = ipWebLogTuple.aggregateByKey(emptyWebLogdataset)(addTodataset, mergePartitionSets)
  Log.info(s"Visitor/IP Count: ${webLogByIP.count}")

  val minTimestamp = (dataset: HashSet[WebLog]) => dataset.map(_.timestamp).min
  //session index is number of session windows between min timestamp in a webLogSet grouped by IP and a webLog timestamp in the webLogSet
  val sessionIndex = (minDate: DateTime, date: DateTime) => Seconds.secondsBetween(minDate, date).getSeconds / SESSION_WINDOW_TIME_IN_SECOND toInt
  //map to (session index, webLog)
  val webLogBySessionIndexTuple = (dataset: HashSet[WebLog], timestamp: DateTime) => dataset.map(webLog => (sessionIndex(timestamp, webLog.timestamp), webLog))
  //then group by session for each WebLogSet grouped by IP (IP, (SessionIndex, WebLogSet))
  val webLogByIPSession = webLogByIP.map { ipWebLogTuple =>
    (ipWebLogTuple._1, ipWebLogTuple._2.groupBy(webLog => sessionIndex(minTimestamp(ipWebLogTuple._2), webLog.timestamp)))
  }.cache

  val addToCount = (count: Int, dataset: HashSet[WebLog]) => count + dataset.size
  val mergeCounts = (count1: Int, count2: Int) => count1 + count2
  val minMaxTimestampTuple = (dataset: HashSet[WebLog]) => {
    val timestampdataset = dataset.map(_.timestamp)
    (timestampdataset.min, timestampdataset.max)
  }
  val duration = (timestamp1: DateTime, timestamp2: DateTime) => Seconds.secondsBetween(timestamp1, timestamp2).getSeconds
  val uniqueUrlVisits = (dataset: HashSet[WebLog]) => dataset.map(_.request._2).toList.distinct.size
  //Sessionize the web log by IP (ip, number of sessions, total hit pages, unique URL visits per session dataset, total session time, session time Data Set)
  val aggregateIPSessionPages = webLogByIPSession.map { ipSession =>
    val ip = ipSession._1 //._1
    val numberOfSessions = ipSession._2.size //._2
    val totalHitPages = ipSession._2.map(_._2).aggregate(0)(addToCount, mergeCounts) //._3
    val uniqueUrlVisitsBySessiondataset = ipSession._2.map(_._2).map(uniqueUrlVisits) //._4
    val sessionTimedataset = ipSession._2.map(_._2).map(dataset => {
      val timestampTuple = minMaxTimestampTuple(dataset)
      duration(timestampTuple._1, timestampTuple._2)
    }) //._6
    val totalSessionTime = sessionTimedataset.sum //._5
    (ip, numberOfSessions, totalHitPages, uniqueUrlVisitsBySessiondataset, totalSessionTime, sessionTimedataset)
  }
  //  aggregateIPSessionPages.foreach(tuple => Log.info(s"Sessionize the web log by IP: $tuple"))

  //the average session time
  val totalSessionTimeSum = aggregateIPSessionPages.map(_._5).sum
  val totalSessionCount = aggregateIPSessionPages.map(_._2).sum
  val averageSessionTime = totalSessionTimeSum / totalSessionCount
    Log.info(s"The average session time the web log by IP: $averageSessionTime seconds")

  //the IPs with the longest total session time by IP
  val longestTotalSessionTimeByIP = aggregateIPSessionPages.map(_._5).max
  Log.info(s"The longest total session time by IP: $longestTotalSessionTimeByIP")
  def ipsWithLongestTotalSessionTimeByIP = aggregateIPSessionPages.filter(_._5 == longestTotalSessionTimeByIP).map(_._1)
  ipsWithLongestTotalSessionTimeByIP.foreach(ip => Log.info(s"The longest total session time IP: $ip"))

  //the IPs with the longest session time by session
  val longestSessionTimeBySession = aggregateIPSessionPages.flatMap(_._6).max
  Log.info(s"The longest session time by session: $longestSessionTimeBySession")
  def ipsWithLongestSessionTimeBySession = aggregateIPSessionPages.filter(_._6.exists(_ == longestSessionTimeBySession)).distinct.map(_._1)
  ipsWithLongestSessionTimeBySession.foreach(ip => Log.info(s"The longest session time by session IP: $ip"))

}

