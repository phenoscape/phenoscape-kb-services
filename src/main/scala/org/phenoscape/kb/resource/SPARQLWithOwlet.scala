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

/**
 * This implements a SPARQL endpoint which preprocesses queries using owlet, before passing the query on 
 * to a "real" SPARQL endpoint.
 */
@Path("sparql")
class SPARQLWithOwlet(@QueryParam("query") queryParam: String, @HeaderParam("Accept") acceptParam: String) {

  val queryOption: Option[String] = Option(queryParam)
  val acceptOption: Option[String] = Option(acceptParam)

  @GET
  @Produces(Array("application/sparql-results+xml", "application/sparql-results+json", "text/tab-separated-values",
    "text/csv", "application/rdf+xml", "text/plain", "application/x-turtle", "text/rdf+n3"))
  def executeQuery(): Response = {
    if (queryOption.isDefined) {
      val expander = new QueryExpander(App.reasoner)
      val expandedQuery = expander.expandQueryString(queryOption.get)
      val client = ClientBuilder.newClient()
      val target = client.target(App.endpoint)
      val form = new Form();
      form.param("query", expandedQuery);
      target.request(acceptOption.get).post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    } else {
      Response.status(Response.Status.BAD_REQUEST).build()
    }
  }

}