package org.phenoscape.kb.resource

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import scala.collection.JavaConversions._
import javax.ws.rs.core.Response
import javax.ws.rs.QueryParam
import javax.ws.rs.DefaultValue
import org.phenoscape.kb.util.App
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP
import javax.ws.rs.HeaderParam
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.Form
import javax.ws.rs.client.Entity
import javax.ws.rs.core.Response.ResponseBuilder
import javax.ws.rs.POST
import javax.ws.rs.Consumes
import scala.util.Try
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.scowl.OWL._
import org.phenoscape.owl.Vocab._
import com.hp.hpl.jena.query.Query
import scala.util.Success
import scala.util.Failure

@Path("genes_affecting_phenotype")
class GenesAffectingPhenotype(@QueryParam("entity") var entityParam: String, @QueryParam("quality") var qualityParam: String) {

  private val entityInput: Try[IRI] = Try(IRI.create(entityParam))
  private val qualityInput: Try[IRI] = Try(IRI.create(qualityParam))

  @Produces(Array("text/tab-separated-values"))
  def urlQuery(): Response = buildResponse match {
    case Success(response) => response
    case Failure(e) => Response.status(Response.Status.BAD_REQUEST).build()
  }

  def buildResponse: Try[Response] = for {
    entityIRI <- entityInput
    qualityIRI <- qualityInput
  } yield {
    val client = ClientBuilder.newClient()
    val target = client.target(App.endpoint)
    val form = new Form()
    form.param("query", buildQuery(entityIRI, qualityIRI).toString)
    val response = target.request("text/tab-separated-values").post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    Response.status(response.getStatus()).entity(response.getEntity()).build()
  }

  def buildQuery(entityIRI: IRI, qualityIRI: IRI): Query = {
    val quality = Class(qualityIRI)
    select_distinct('gene, 'gene_label, 'taxon_label, 'source) from "http://kb.phenoscape.org/" where (
      bgp(
        t('pheno_instance, rdfType, 'phenotype),
        t('pheno_instance, associated_with_taxon, 'taxon),
        t('taxon, rdfsLabel, 'taxon_label),
        t('pheno_instance, associated_with_gene, 'gene),
        t('gene, rdfsLabel, 'gene_label)),
        optional(bgp(
          t('pheno_instance, dcSource, 'source))),
        service(App.owlery, bgp(
          t('phenotype, rdfsSubClassOf, ((has_part some (quality and (inheres_in_part_of some Class(entityIRI)))) or (has_part some (quality and (TOWARDS some Class(entityIRI)))) or (has_part some (quality and (TOWARDS value Individual(entityIRI))))).asOMN))))
  }

}