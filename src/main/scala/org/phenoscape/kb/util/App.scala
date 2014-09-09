package org.phenoscape.kb.util

import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype

/**
 * Caution—this object must be safe to access from multiple threads.
 */
object App {

  private val Prior = IRI.create("http://www.bigdata.com/queryHints#Prior")
  private val RunFirst = IRI.create("http://www.bigdata.com/queryHints#runFirst")

  val KB_SPARQL_ENDPOINT_PROPERTY = "org.phenoscape.kb.endpoint"
  val KB_OWLERY_ENDPOINT_PROPERTY = "org.phenoscape.owlery.endpoint"

  val endpoint: String = System.getProperty(KB_SPARQL_ENDPOINT_PROPERTY)
  val owlery: String = System.getProperty(KB_OWLERY_ENDPOINT_PROPERTY)

  val BigdataRunPriorFirst = bgp(t(Prior, RunFirst, "true" ^^ XSDDatatype.XSDboolean))

}

