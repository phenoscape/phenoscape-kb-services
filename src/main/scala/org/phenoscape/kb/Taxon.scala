package org.phenoscape.kb

import org.semanticweb.owlapi.model.IRI
import spray.json.DefaultJsonProtocol._
import spray.json._

object Taxon {

}

case class Taxon(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject
  }

}