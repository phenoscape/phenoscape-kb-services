package org.phenoscape.kb.resource

import javax.ws.rs.Path
import javax.ws.rs.Produces
import java.io.BufferedWriter
import javax.ws.rs.core.StreamingOutput
import javax.ws.rs.GET
import javax.ws.rs.client.ClientBuilder
import java.io.OutputStream
import javax.ws.rs.core.Response
import java.io.OutputStreamWriter
import org.phenoscape.kb.util.App
import javax.ws.rs.QueryParam
import org.phenoscape.scowl.OWL._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import com.hp.hpl.jena.query.Query
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.apibinding.OWLManager
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP
import scala.collection.JavaConversions._
import com.google.common.collect.HashMultiset
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await

@Path("annotation/quality")
class NearestQuality(@QueryParam("annotation") var annotationIDParam: String) {

  private val annotationIDOption: Option[String] = Option(annotationIDParam)
  val factory = OWLManager.getOWLDataFactory
  val rdfType = ObjectProperty(Vocab.rdfType)
  val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
  val rdfsIsDefinedBy = factory.getRDFSIsDefinedBy
  val PATO = IRI.create("http://purl.obolibrary.org/obo/pato.owl")

  @GET
  @Produces(Array("text/plain"))
  def query(): String = {
    val annotationID = annotationIDOption.getOrElse(???)
    val query = superTypeQuery(annotationID)
    val queryEngine = new QueryEngineHTTP(App.endpoint, query);
    val resultSet = queryEngine.execSelect
    val superclasses = HashMultiset.create[String]
    val superClassQueries = Future.sequence(resultSet map { superClass =>
      Future {
        val subquery = superClassQuery(superClass.getResource("quality").getURI)
        val subqueryEngine = new QueryEngineHTTP(App.endpoint, subquery);
        val subResultSet = blocking { subqueryEngine.execSelect }
        subqueryEngine.close()
        subResultSet map (_.getResource("quality").getURI)
      }
    })
    Await.result(superClassQueries, 10 minutes).flatten foreach { superclass =>
      superclasses.add(superclass)
    }
    queryEngine.close()
    val nearestQualities = superclasses.entrySet filter (_.getCount == 1) map (_.getElement)
    nearestQualities.mkString("\n")
  }

  def superTypeQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    select_distinct('quality) from "http://kb.phenoscape.org/" where (
      bgp(
        t(annotationIRI, rdfType / (rdfsSubClassOf*), 'quality),
        t('quality, rdfsIsDefinedBy, PATO)))
  }

  def superClassQuery(termID: String): Query = {
    val termIRI = IRI.create(termID)
    select_distinct('quality) from "http://kb.phenoscape.org/" where (
      bgp(
        t(termIRI, rdfsSubClassOf*, 'quality),
        t('quality, rdfsIsDefinedBy, PATO)))
  }

}