package org.phenoscape.kb.resource

import scala.collection.JavaConversions._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.log4j.Logger
import org.phenoscape.kb.util.App
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owl.Vocab._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLEntity
import org.semanticweb.owlapi.vocab.DublinCoreVocabulary
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary
import org.phenoscape.scowl.OWL._
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.expr.E_LessThan
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock
import com.hp.hpl.jena.graph.Triple
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.Entity
import javax.ws.rs.core.Form
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import com.hp.hpl.jena.sparql.core.Var

@Path("entity/presence")
class PresenceOfStructure(@QueryParam("taxon") var taxonParam: String, @QueryParam("entity") var entityParam: String, @HeaderParam("Accept") acceptParam: String) {

  private val entityInput: Try[IRI] = Try(IRI.create(entityParam))
  private val taxonInput: Try[IRI] = Try(IRI.create(taxonParam))
  private val acceptOption: Option[String] = Option(acceptParam)

  @GET
  @Produces(Array("text/tab-separated-values", "application/sparql-results+json", "application/json"))
  def urlQuery(): Response = {
    val inputs = for {
      entityIRI <- entityInput
      taxonIRI <- taxonInput
    } yield {
      (entityIRI, taxonIRI)
    }
    inputs match {
      case Success((entityIRI, taxonIRI)) => {
        val client = ClientBuilder.newClient()
        val target = client.target(App.endpoint)
        val form = new Form()
        form.param("query", buildQuery(taxonIRI, entityIRI).toString)
        val response = target.request(acceptOption.getOrElse("text/tab-separated-values")).post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
        Response.status(response.getStatus()).entity(response.getEntity()).build()
      }
      case Failure(e) => Response.status(Response.Status.BAD_REQUEST).build()
    }
  }

  def buildQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
    val taxon = Class(taxonIRI)
    val entity = Class(entityIRI)
    select_distinct('state, 'state_label, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, HAS_MEMBER / EXHIBITS / rdfType, 'phenotype),
        t('state, DENOTES_EXHIBITING / rdfType, 'phenotype),
        t('state, dcDescription, 'state_label),
        t('matrix, HAS_CHARACTER, 'matrix_char),
        t('matrix, rdfsLabel, 'matrix_label),
        t('matrix_char, MAY_HAVE_STATE_VALUE, 'state)),
        service(App.owlery, bgp(
          t('phenotype, rdfsSubClassOf, (IMPLIES_PRESENCE_OF some entity).asOMN))))
  }

  lazy val logger = Logger.getLogger(this.getClass)

}