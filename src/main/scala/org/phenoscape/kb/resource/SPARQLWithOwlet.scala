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
import org.apache.log4j.Logger
import com.hp.hpl.jena.query.Query

/**
 * This implements a SPARQL endpoint which preprocesses queries using owlet, before passing the query on
 * to a "real" SPARQL endpoint.
 */
@Path("sparql")
class SPARQLWithOwlet(@QueryParam("query") queryParam: String, @HeaderParam("Accept") acceptParam: String) {

  private val queryOption: Option[String] = Option(queryParam)
  private val acceptOption: Option[String] = Option(acceptParam)

  @GET
  @Produces(Array("application/sparql-results+xml", "application/sparql-results+json", "text/tab-separated-values",
    "text/csv", "application/rdf+xml", "text/plain", "application/x-turtle", "text/rdf+n3"))
  def urlQuery(): Response = {
    if (queryOption.isDefined) {
      executeQuery(QueryFactory.create(queryOption.get))
    } else {
      Response.status(Response.Status.BAD_REQUEST).build()
    }
  }

  @POST
  @Consumes(Array("application/sparql-query"))
  @Produces(Array("application/sparql-results+xml", "application/sparql-results+json", "text/tab-separated-values",
    "text/csv", "application/rdf+xml", "text/plain", "application/x-turtle", "text/rdf+n3"))
  def directPOSTQuery(query: Query): Response = {
    //TODO query should be interpreted as UTF-8; where is this determined?
    if (queryOption.isDefined) {
      Response.status(Response.Status.BAD_REQUEST).build()
    } else {
      logger.error("Parsing query: " + query)
      executeQuery(query)
    }
  }

  def executeQuery(query: Query): Response = {
    val expander = new QueryExpander(App.reasoner)
    val expandedQuery = expander.expandQuery(query)
    val client = ClientBuilder.newClient()
    val target = client.target(App.endpoint)
    val form = new Form()
    form.param("query", expandedQuery.toString)
    // accept doesn't work if empty
    val response = target.request(acceptOption.getOrElse("")).post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    // Don't understand why we can't just pass along the response object
    //Response.fromResponse(response).entity(response.getEntity()).build()
    Response.status(response.getStatus()).entity(response.getEntity()).build()

  }
  
  lazy val logger = Logger.getLogger(this.getClass)

}