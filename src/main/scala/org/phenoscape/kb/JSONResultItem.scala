package org.phenoscape.kb

import spray.json._

trait JSONResultItem {

  def toJSON: JsObject

}