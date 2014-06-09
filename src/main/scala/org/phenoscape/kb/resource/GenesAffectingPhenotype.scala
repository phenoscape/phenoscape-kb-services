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
import org.phenoscape.owlet.QueryExpander
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

@Path("genes_affecting_phenotype")
class GenesAffectingPhenotype(@QueryParam("entity") entityParam: String, @QueryParam("quality") qualityParam: String) {

  private val entityOption: Option[String] = Option(entityParam)
  private val qualityOption: Option[String] = Option(qualityParam)
  private val query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX ps: <http://purl.org/phenoscape/vocab.owl#>
PREFIX ow: <http://purl.org/phenoscape/owlet/syntax#>
PREFIX StandardState: <http://purl.obolibrary.org/obo/CDAO_0000045>
PREFIX has_character: <http://purl.obolibrary.org/obo/CDAO_0000142>
PREFIX has_state: <http://purl.obolibrary.org/obo/CDAO_0000184>
PREFIX belongs_to_tu: <http://purl.obolibrary.org/obo/CDAO_0000191>
PREFIX has_external_reference: <http://purl.obolibrary.org/obo/CDAO_0000164>
PREFIX part_of: <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX has_part: <http://purl.obolibrary.org/obo/BFO_0000051>
PREFIX inheres_in: <http://purl.obolibrary.org/obo/BFO_0000052>
PREFIX Entity: <??ENTITY_IRI>
PREFIX Quality: <??QUALITY_IRI>
PREFIX towards: <http://purl.obolibrary.org/obo/pato#towards>

SELECT DISTINCT ?gene (STR(?gene_label) AS ?gene_label_string) (STR(?taxon_label) AS ?taxon_label_string) ?source
FROM <http://kb.phenoscape.org/>
WHERE
{
?eq rdfs:subClassOf "((has_part: some Quality:) and (inheres_in: some Entity:)) or ((has_part: some Quality:) and (towards: some Entity:)) or ((has_part some Quality:) and (towards: value Entity:))"^^ow:omn .
?pheno_instance rdf:type ?eq .
?pheno_instance ps:associated_with_taxon ?taxon .
?taxon rdfs:label ?taxon_label .
?pheno_instance ps:associated_with_gene ?gene .
?gene rdfs:label ?gene_label .
OPTIONAL {
  ?pheno_instance dc:source ?source .
}
}
"""

  @GET
  @Produces(Array("text/tab-separated-values"))
  def urlQuery(): Response = {
    if (entityOption.isDefined && qualityOption.isDefined) {
      val expander = new QueryExpander(App.reasoner)
      val expandedQuery = expander.expandQueryString(
        query.replaceAllLiterally("??ENTITY_IRI", entityOption.get).replaceAllLiterally("??QUALITY_IRI", qualityOption.get))
      val client = ClientBuilder.newClient()
      val target = client.target(App.endpoint)
      val form = new Form()
      form.param("query", expandedQuery)
      val response = target.request("text/tab-separated-values").post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
      Response.status(response.getStatus()).entity(response.getEntity()).build()
    } else {
      Response.status(Response.Status.BAD_REQUEST).build()
    }
  }

}