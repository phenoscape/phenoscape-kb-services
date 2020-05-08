package org.phenoscape.kb

import org.apache.jena.query.ResultSet

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller

import spray.json._
import spray.json.DefaultJsonProtocol._

object ResultCount {

  def apply(resultSet: ResultSet): ResultCount = ResultCount(count(resultSet))

  def count(resultSet: ResultSet): Int =
    if (resultSet.hasNext) resultSet.next.getLiteral("count").getInt
    else 0

  implicit val ResultCountMarshaller: ToEntityMarshaller[ResultCount] =
    Marshaller.combined(result => result.toJSON)

}

case class ResultCount(count: Int) {

  def toJSON: JsObject =
    Map("total" -> count).toJson.asJsObject

}
