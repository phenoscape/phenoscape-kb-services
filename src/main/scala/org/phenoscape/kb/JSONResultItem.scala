package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

trait JSONResultItem {

  def toJSON: JsObject

}

object JSONResultItem {

  implicit val marshaller: ToEntityMarshaller[JSONResultItem] = Marshaller.combined(_.toJSON)

}