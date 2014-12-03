package org.phenoscape.kb

import scala.concurrent.Future
import scala.concurrent.blocking
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.query.ResultSetFactory
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP
import com.typesafe.config.ConfigFactory
import com.hp.hpl.jena.query.QuerySolution
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.core.TriplePath
import com.hp.hpl.jena.sparql.syntax.ElementService
import spray.http._
import com.hp.hpl.jena.query.ResultSetFormatter
import java.io.ByteArrayOutputStream
import spray.json.JsValue
import spray.json.JsObject
import spray.client.pipelining._
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import Main.system
import system.dispatcher
import com.hp.hpl.jena.query.QueryFactory
import spray.can.Http
import com.hp.hpl.jena.rdf.model.Model
import org.apache.jena.riot.RDFDataMgr
import com.hp.hpl.jena.rdf.model.ModelFactory
import java.io.ByteArrayInputStream

object App {

  private val Prior = IRI.create("http://www.bigdata.com/queryHints#Prior")
  private val RunFirst = IRI.create("http://www.bigdata.com/queryHints#runFirst")
  val BigdataRunPriorFirst = bgp(t(Prior, RunFirst, "true" ^^ XSDDatatype.XSDboolean))
  val `application/sparql-query` = MediaTypes.register(MediaType.custom("application/sparql-query"))
  val `application/sparql-results+xml` = MediaTypes.register(MediaType.custom("application/sparql-results+xml"))
  val `application/rdf+xml` = MediaTypes.register(MediaType.custom("application/rdf+xml"))

  val conf = ConfigFactory.load()
  val KBEndpoint = IRI.create(conf.getString("kb-services.kb.endpoint"))
  val OwleryEndpoint = IRI.create(conf.getString("kb-services.owlery.endpoint"))

  val `application/ld+json` = MediaTypes.register(MediaType.custom("application/ld+json"))

  def withOwlery(triple: TripleOrPath): ElementService = service(App.OwleryEndpoint.toString, bgp(triple))

  def executeSPARQLQuery(query: Query): Future[ResultSet] = sparqlSelectQuery(Post(App.KBEndpoint.toString, query))

  def executeSPARQLQuery[T](query: Query, resultMapper: QuerySolution => T): Future[Seq[T]] = for {
    resultSet <- sparqlSelectQuery(Post(App.KBEndpoint.toString, query))
  } yield {
    resultSet.map(resultMapper).toSeq
  }

  def executeSPARQLConstructQuery(query: Query): Future[Model] = sparqlConstructQuery(Post(App.KBEndpoint.toString, query))

  def resultSetToTSV(result: ResultSet): String = {
    val outStream = new ByteArrayOutputStream()
    ResultSetFormatter.outputAsTSV(outStream, result)
    val tsv = outStream.toString("utf-8")
    outStream.close()
    tsv
  }

  private implicit val SPARQLQueryMarshaller = Marshaller.delegate[Query, String](`application/sparql-query`, MediaTypes.`text/plain`)(_.toString)
  private implicit val SPARQLQueryBodyUnmarshaller = Unmarshaller.delegate[String, Query](`application/sparql-query`)(QueryFactory.create)
  private implicit val SPARQLResultsXMLUnmarshaller = Unmarshaller.delegate[String, ResultSet](`application/sparql-results+xml`)(ResultSetFactory.fromXML)
  private implicit val RDFXMLUnmarshaller = Unmarshaller.delegate[String, Model]() { text =>
    val model = ModelFactory.createDefaultModel
    model.read(new ByteArrayInputStream(text.getBytes), null)
    model
  }

  def expandWithOwlet(query: Query): Future[Query] = {
    val pipeline = sendReceive ~> unmarshal[Query]
    pipeline(Post("http://pkb-new.nescent.org/owlery/kbs/phenoscape/expand", query))
  }

  val sparqlSelectQuery: HttpRequest => Future[ResultSet] = addHeader(HttpHeaders.Accept(`application/sparql-results+xml`)) ~> sendReceive ~> unmarshal[ResultSet]
  val sparqlConstructQuery: HttpRequest => Future[Model] = addHeader(HttpHeaders.Accept(`application/rdf+xml`)) ~> sendReceive ~> unmarshal[Model]

}