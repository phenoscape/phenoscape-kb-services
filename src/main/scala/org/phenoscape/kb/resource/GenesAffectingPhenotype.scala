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
class GenesAffectingPhenotype(@QueryParam("iri") iriParam: String) {

  private val iriOption: Option[String] = Option(iriParam)
  private val query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX ps: <http://purl.org/phenoscape/vocab/>
PREFIX ow: <http://purl.org/phenoscape/owlet/syntax#>
PREFIX StandardState: <http://purl.obolibrary.org/obo/CDAO_0000045>
PREFIX has_character: <http://purl.obolibrary.org/obo/CDAO_0000142>
PREFIX has_state: <http://purl.obolibrary.org/obo/CDAO_0000184>
PREFIX belongs_to_tu: <http://purl.obolibrary.org/obo/CDAO_0000191>
PREFIX has_external_reference: <http://purl.obolibrary.org/obo/CDAO_0000164>
PREFIX HasNumberOf: <http://purl.obolibrary.org/obo/PATO_0001555>
PREFIX Count: <http://purl.obolibrary.org/obo/PATO_0000070>
PREFIX part_of: <http://purl.obolibrary.org/obo/BFO_0000050>
PREFIX inheres_in: <http://purl.obolibrary.org/obo/BFO_0000052>
PREFIX LimbFin: <http://purl.obolibrary.org/obo/UBERON_0004708>
PREFIX Sarcopterygii: <http://purl.obolibrary.org/obo/VTO_0001464>
PREFIX Entity: <??ENTITY_IRI>
PREFIX towards: <http://purl.obolibrary.org/obo/pato#towards>

SELECT DISTINCT ?gene (STR(?gene_label) AS ?gene_label_string) (STR(?taxon_label) AS ?taxon_label_string)
FROM <http://kb.phenoscape.org/>
WHERE
{
?eq rdfs:subClassOf "(Count: and inheres_in: some Entity:) or (HasNumberOf: and towards: value Entity:)"^^ow:omn .
?pheno_instance rdf:type ?eq .
?pheno_instance ps:annotated_taxon ?taxon .
?taxon rdfs:label ?taxon_label .
?pheno_instance ps:annotated_gene ?gene .
?gene rdfs:label ?gene_label .
}
"""

  @GET
  @Produces(Array("text/tab-separated-values"))
  def urlQuery(): Response = {
    if (iriOption.isDefined) {
      val expander = new QueryExpander(App.reasoner)
      val expandedQuery = expander.expandQueryString(query.replaceAllLiterally("??ENTITY_IRI", iriOption.get))
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