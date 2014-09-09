package org.phenoscape.kb.resource

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.Entity
import javax.ws.rs.core.Form
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import scala.collection.JavaConversions._
import org.phenoscape.owl.Vocab._
import org.semanticweb.owlapi.model.OWLEntity
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary
import org.semanticweb.owlapi.vocab.DublinCoreVocabulary
import org.phenoscape.kb.util.App
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.apache.log4j.Logger
import org.semanticweb.owlapi.model.OWLClassExpression
import org.phenoscape.scowl.OWL._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType._
import com.hp.hpl.jena.query.Query
import javax.ws.rs.HeaderParam

@Path("entity/absence")
class AbsenceOfStructure(@QueryParam("taxon") var taxonParam: String, @QueryParam("entity") var entityParam: String, @HeaderParam("Accept") acceptParam: String) {

  private val entityInput: Try[IRI] = Try(IRI.create(entityParam))
  private val taxonInput: Try[IRI] = Try(IRI.create(taxonParam))
  private val acceptOption: Option[String] = Option(acceptParam)

  @GET
  @Produces(Array("text/tab-separated-values", "application/sparql-results+json", "application/json"))
  def urlQuery(): Response = buildResponse match {
    case Success(response) => response
    case Failure(e) => Response.status(Response.Status.BAD_REQUEST).build()
  }

  def buildResponse: Try[Response] = for {
    entityIRI <- entityInput
    taxonIRI <- taxonInput
  } yield {
    val client = ClientBuilder.newClient()
    val target = client.target(App.endpoint)
    val form = new Form()
    form.param("query", buildQuery(taxonIRI, entityIRI).toString)
    val response = target.request(acceptOption.getOrElse("text/tab-separated-values")).post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    Response.status(response.getStatus()).entity(response.getEntity()).build()
  }

  def buildQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
    val taxon = Class(taxonIRI)
    val entity = Individual(entityIRI)
    select_distinct('state, 'state_label, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, HAS_MEMBER / EXHIBITS / rdfType, 'phenotype),
        t('state, DENOTES_EXHIBITING / rdfType, 'phenotype),
        t('state, dcDescription, 'state_label),
        t('matrix, HAS_CHARACTER, 'matrix_char),
        t('matrix, rdfsLabel, 'matrix_label),
        t('matrix_char, MAY_HAVE_STATE_VALUE, 'state)),
        service(App.owlery, bgp(
          t('phenotype, rdfsSubClassOf, (LacksAllPartsOfType and (TOWARDS value entity) and (inheres_in some MultiCellularOrganism)).asOMN))),
        App.BigdataRunPriorFirst)
  }

  lazy val logger = Logger.getLogger(this.getClass)

}