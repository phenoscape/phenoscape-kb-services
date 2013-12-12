package org.phenoscape.kb.resource

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.HeaderParam
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
import org.phenoscape.owlet.QueryExpander
import org.phenoscape.kb.util.App
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import org.apache.log4j.Logger

@Path("entity/presence")
class PresenceOfStructure(@QueryParam("taxon") var taxonParam: String, @QueryParam("entity") var entityParam: String, @HeaderParam("Accept") acceptParam: String) {

  private val entityInput: Try[IRI] = Try(IRI.create(entityParam))
  private val taxonInput: Try[IRI] = Try(IRI.create(taxonParam))
  private val acceptOption: Option[String] = Option(acceptParam)

  private def u(entity: OWLEntity): String = u(entity.getIRI)
  private def u(iri: IRI): String = u(iri.toString)
  private def u(text: String): String = s"<${text}>"
  val rdfType = OWLRDFVocabulary.RDF_TYPE.getIRI
  val rdfsSubClassOf = OWLRDFVocabulary.RDFS_SUBCLASS_OF.getIRI
  val rdfsLabel = OWLManager.getOWLDataFactory.getRDFSLabel
  val dcDescription = DublinCoreVocabulary.DESCRIPTION.getIRI

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
        val expander = new QueryExpander(App.reasoner)
        println(query.replaceAllLiterally("?taxon!", u(taxonIRI)).replaceAllLiterally("?entity!", u(entityIRI)))
        val expandedQuery = expander.expandQueryString(
          query.replaceAllLiterally("?taxon!", u(taxonIRI)).replaceAllLiterally("?entity!", u(entityIRI)))
        val client = ClientBuilder.newClient()
        val target = client.target(App.endpoint)
        val form = new Form()
        form.param("query", expandedQuery)
        val response = target.request(acceptOption.getOrElse("text/tab-separated-values")).post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
        Response.status(response.getStatus()).entity(response.getEntity()).build()
      }
      case Failure(e) => Response.status(Response.Status.BAD_REQUEST).build()
    }
  }

  private val query = s"""
PREFIX ow: <http://purl.org/phenoscape/owlet/syntax#>
SELECT DISTINCT ?state (STR(?state_label_text) AS ?state_label) (STR(?matrix_label_text) AS ?matrix_label)
FROM <http://kb.phenoscape.org/>
WHERE
{
?taxon! ${u(HAS_MEMBER)}/${u(EXHIBITS)} ?pheno_instance .
?pheno_instance ${u(rdfType)} ?phenotype .
?phenotype ${u(rdfsSubClassOf)} "${u(IMPLIES_PRESENCE_OF)} some ?entity!"^^ow:omn .
?state ${u(DENOTES_EXHIBITING)} ?state_instance .
?state_instance ${u(rdfType)} ?phenotype .
?state ${u(rdfType)} ${u(STANDARD_STATE)} .
?state ${u(dcDescription)} ?state_label_text .
?matrix ${u(HAS_CHARACTER)} ?matrix_char .
?matrix ${u(rdfsLabel)} ?matrix_label_text .
?matrix_char ${u(MAY_HAVE_STATE_VALUE)} ?state .
}
"""

  lazy val logger = Logger.getLogger(this.getClass)

}