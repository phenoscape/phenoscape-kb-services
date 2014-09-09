package org.phenoscape.kb.resource

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.phenoscape.kb.util.App
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI

import com.hp.hpl.jena.query.Query

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.Entity
import javax.ws.rs.core.Form
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import javax.ws.rs.QueryParam

@Path("genes_affecting_count_phenotype")
class GenesAffectingCountPhenotype(@QueryParam("iri") var iriParam: String) {

  private val entityInput: Try[IRI] = Try(IRI.create(iriParam))
  private val Count = Class("http://purl.obolibrary.org/obo/PATO_0000070")
  private val HasNumberOf = Class(HAS_NUMBER_OF)

  @GET
  @Produces(Array("text/tab-separated-values"))
  def urlQuery(): Response = buildResponse match {
    case Success(response) => response
    case Failure(e) => Response.status(Response.Status.BAD_REQUEST).build()
  }

  def buildResponse: Try[Response] = for {
    entityIRI <- entityInput
  } yield {
    val client = ClientBuilder.newClient()
    val target = client.target(App.endpoint)
    val form = new Form()
    form.param("query", buildQuery(entityIRI).toString)
    val response = target.request("text/tab-separated-values").post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    Response.status(response.getStatus()).entity(response.getEntity()).build()
  }

  def buildQuery(entityIRI: IRI): Query = {
    val entity = Individual(entityIRI)
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
          t('phenotype, rdfsSubClassOf, ((has_part some (Count and (inheres_in some Class(entityIRI)))) or (has_part some (HasNumberOf and (TOWARDS value Individual(entityIRI))))).asOMN))),
        App.BigdataRunPriorFirst)
  }

}