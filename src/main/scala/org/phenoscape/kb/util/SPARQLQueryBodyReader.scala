package org.phenoscape.kb.util

import javax.ws.rs.ext.MessageBodyReader
import com.hp.hpl.jena.query.Query
import java.lang.reflect.Type
import java.lang.annotation.Annotation
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.MultivaluedMap
import java.io.InputStream
import javax.ws.rs.ext.Provider
import scala.io.Source
import com.hp.hpl.jena.query.QueryFactory

@Provider
class SPARQLQueryBodyReader extends MessageBodyReader[Query] {

  def isReadable(aType: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType): Boolean = {
    (aType == classOf[Query]) && (mediaType == "application/sparql-query")
    true
  }

  def readFrom(aType: Class[Query], genericType: Type, annotations: Array[Annotation], mediaType: MediaType, httpHeaders: MultivaluedMap[String, String], entityStream: InputStream): Query = {
    val queryString = Source.fromInputStream(entityStream, "utf-8").mkString
    QueryFactory.create(queryString)
  }

}