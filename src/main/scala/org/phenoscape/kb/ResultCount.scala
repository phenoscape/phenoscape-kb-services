package org.phenoscape.kb

import com.hp.hpl.jena.query.ResultSet
import spray.json.DefaultJsonProtocol._
import spray.json._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.http._
import spray.httpx._

object ResultCount {

  def apply(resultSet: ResultSet): ResultCount = ResultCount(count(resultSet))

  def count(resultSet: ResultSet): Int =
    if (resultSet.hasNext) resultSet.next.getLiteral("count").getInt
    else 0

  implicit val ResultCountMarshaller = Marshaller.delegate[ResultCount, JsObject](MediaTypes.`application/json`) { result =>
    result.toJSON
  }

}

case class ResultCount(count: Int) {

  def toJSON: JsObject = {
    Map("total" -> count).toJson.asJsObject
  }

}