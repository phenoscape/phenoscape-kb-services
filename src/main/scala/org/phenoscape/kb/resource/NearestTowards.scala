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
import org.phenoscape.owl.NamedRestrictionGenerator
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await

@Path("annotation/towards")
class NearestTowards(@QueryParam("annotation") annotationIDParam: String) {

  private val annotationIDOption: Option[String] = Option(annotationIDParam)
  val factory = OWLManager.getOWLDataFactory
  val rdfType = ObjectProperty(Vocab.rdfType)
  val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
  val rdfsIsDefinedBy = factory.getRDFSIsDefinedBy
  val towardsSome = NamedRestrictionGenerator.getClassRelationIRI(Vocab.TOWARDS.getIRI)
  val UBERON = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")

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
        val subquery = superClassQuery(superClass.getResource("description").getURI)
        val subqueryEngine = new QueryEngineHTTP(App.endpoint, subquery);
        val subResultSet = blocking { subqueryEngine.execSelect }
        subqueryEngine.close()
        subResultSet map (_.getResource("bearer").getURI)
      }
    })
    Await.result(superClassQueries, 10 minutes).flatten foreach { superclass =>
      superclasses.add(superclass)
    }
    queryEngine.close()
    val nearestBearers = superclasses.entrySet filter (_.getCount == 1) map (_.getElement)
    nearestBearers.mkString("\n")
  }

  def superTypeQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    select_distinct('description) from "http://kb.phenoscape.org/" where (
      bgp(
        t(annotationIRI, rdfType / (rdfsSubClassOf*), 'description),
        t('description, towardsSome, 'bearer),
        t('bearer, rdfsIsDefinedBy, UBERON)))
  }

  def superClassQuery(termID: String): Query = {
    val termIRI = IRI.create(termID)
    select_distinct('bearer) from "http://kb.phenoscape.org/" where (
      bgp(
        t(termIRI, rdfsSubClassOf*, 'description),
        t('description, towardsSome, 'bearer),
        t('bearer, rdfsIsDefinedBy, UBERON)))
  }

}