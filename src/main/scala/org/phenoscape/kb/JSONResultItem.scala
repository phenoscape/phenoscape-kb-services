package org.phenoscape.kb

import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import spray.json.DefaultJsonProtocol._
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

trait JSONResultItem {

  def toJSON: JsObject

}

object JSONResultItem {

  implicit val marshaller: ToEntityMarshaller[JSONResultItem] = Marshaller.combined(_.toJSON)

  implicit val JSONResultItemsMarshaller: ToEntityMarshaller[Seq[JSONResultItem]] = Marshaller.combined(results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson)))

  val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json().withFramingRenderer(
    Flow[ByteString].intersperse(ByteString("{\"results\":["), ByteString(","), ByteString("]}"))
  )

}