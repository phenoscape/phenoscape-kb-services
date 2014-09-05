package org.phenoscape.kb.resource

import org.phenoscape.kb.util.App

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.Entity
import javax.ws.rs.core.Form
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import scala.util.Try
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.scowl.OWL._
import org.phenoscape.owl.Vocab._
import com.hp.hpl.jena.query.Query
import scala.util.Success
import scala.util.Failure

@Path("genes_expressed_in_structure")
class GenesExpressedWithinStructure(@QueryParam("iri") iriParam: String) {

  private val entityInput: Try[IRI] = Try(IRI.create(iriParam))
  private val iriOption: Option[String] = Option(iriParam)

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
    val entity = Class(entityIRI)
    select_distinct('gene, 'gene_label, 'taxon_label, 'source) from "http://kb.phenoscape.org/" where (
      bgp(
        t('structure, rdfType, 'entity),
        t('expression, OCCURS_IN, 'structure),
        t('expression, rdfType, GeneExpression),
        t('expression, associated_with_taxon, 'taxon),
        t('taxon, rdfsLabel, 'taxon_label),
        t('expression, associated_with_gene, 'gene),
        t('gene, rdfsLabel, 'gene_label)),
        optional(bgp(
          t('expression, dcSource, 'source))),
        service(App.owlery, bgp(
          t('entity, rdfsSubClassOf, (part_of some entity).asOMN))))
  }

}