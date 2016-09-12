package weblog

import java.sql.Timestamp

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/**
  * Created by kai.zhu on 2016-09-08.
  */
case class WebLog(
                   timestamp: DateTime, elb: String, client_port: (String, String), backend_port: (String, String),
                   requestProcessingTime: Double, backendProcessingTime: Double, responseProcessingTime: Double,
                   elbStatusCode: String, backendStatusCode: String,
                   receivedBytes: Int, sentBytes: Int,
                   request: (String, String, String), userAgent: String,
                   sslCipher: String, sslProtocl: String) {

}

object WebLogHelper {

  val SESSION_WINDOW_TIME_IN_SECOND = 15 * 60D

  implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

  implicit class StringToDate(value: String) {
    def toDateTime = {
      ISODateTimeFormat.dateTime().parseDateTime(this.value)
    }
  }

  implicit class StringToTimestamp(value: String) {
    def toTimestamp = {
      Timestamp.from(this.value.toDateTime.toDate.toInstant)
    }
  }

  implicit def StringToTuple2(value: String): (String, String) = {
    val values = value.split(":")
    (values(0), if (values.size == 2) values(1) else "")
  }

  //  val log = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
  val pattern = new scala.util.matching.Regex(
    """(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s(\S*)\s("(\S*)\s(\S*)\s(\S*)")\s(".*")\s(\S*)\s(\S*)""",
    "timestamp", "elb", "client_port", "backend_port",
    "requestProcessingTime", "backendProcessingTime", "responseProcessingTime",
    "elbStatusCode", "backendStatusCode",
    "receivedBytes", "sentBytes",
    "request", "httpMethod", "url", "httpVersion", "userAgent",
    "sslCipher", "sslProtocl")

  def toWebLog(log: String): Option[WebLog] = {
    pattern findFirstMatchIn log match {
      case Some(pattern(
      timestamp, elb, client_port, backend_port,
      requestProcessingTime, backendProcessingTime, responseProcessingTime,
      elbStatusCode, backendStatusCode,
      receivedBytes, sentBytes,
      _, httpMethod, url, httpVersion, userAgent,
      sslCipher, sslProtocl)) => {
        Some(WebLog(
          timestamp.toDateTime, elb, client_port, backend_port,
          requestProcessingTime.toDouble, backendProcessingTime.toDouble, responseProcessingTime.toDouble,
          elbStatusCode, backendStatusCode,
          receivedBytes.toInt, sentBytes.toInt,
          (httpMethod, url, httpVersion), userAgent,
          sslCipher, sslProtocl))
      }
      case None => None
    }
  }

  def toWebLogTuple(log: String) = {
    pattern findFirstMatchIn log match {
      case Some(pattern(
      timestamp, elb, client_port, backend_port,
      requestProcessingTime, backendProcessingTime, responseProcessingTime,
      elbStatusCode, backendStatusCode,
      receivedBytes, sentBytes,
      _, httpMethod, url, httpVersion, userAgent,
      sslCipher, sslProtocl)) => {
        Some((
          timestamp.toTimestamp, elb, client_port._1, client_port._2, backend_port._1, backend_port._2,
          requestProcessingTime.toDouble, backendProcessingTime.toDouble, responseProcessingTime.toDouble,
          elbStatusCode, backendStatusCode,
          receivedBytes.toInt, sentBytes.toInt,
          httpMethod, url, httpVersion, userAgent,
          sslCipher, sslProtocl))
      }
      case None => None
    }
  }

}