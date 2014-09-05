package org.phenoscape.kb.util

/**
 * Caution—this object must be safe to access from multiple threads.
 */
object App {

  val KB_SPARQL_ENDPOINT_PROPERTY = "org.phenoscape.kb.endpoint"
  val KB_OWLERY_ENDPOINT_PROPERTY = "org.phenoscape.owlery.endpoint"

  val endpoint: String = System.getProperty(KB_SPARQL_ENDPOINT_PROPERTY)
  val owlery: String = System.getProperty(KB_OWLERY_ENDPOINT_PROPERTY)
  
}

