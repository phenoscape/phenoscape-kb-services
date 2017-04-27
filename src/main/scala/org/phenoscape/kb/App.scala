package org.phenoscape.kb

import scala.concurrent.Future
import scala.concurrent.blocking
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

import spray.http._

import java.io.ByteArrayOutputStream
import spray.json.JsValue
import spray.json.JsObject
import spray.client.pipelining._
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import Main.system
import system.dispatcher

import spray.can.Http

import org.apache.jena.riot.RDFDataMgr

import java.io.ByteArrayInputStream
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import org.apache.jena.query.ResultSetFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.QuerySolution
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.sparql.syntax.ElementService
import org.apache.jena.query.ResultSet
import org.apache.jena.query.Query
import org.apache.jena.rdf.model.Model

object App {

  implicit val timeout = Timeout(10 minutes)
  private val Prior = IRI.create("http://www.bigdata.com/queryHints#Prior")
  private val RunFirst = IRI.create("http://www.bigdata.com/queryHints#runFirst")
  private val HintQuery = IRI.create("http://www.bigdata.com/queryHints#Query")
  private val HintAnalytic = IRI.create("http://www.bigdata.com/queryHints#analytic")
  private val HintOptimizer = IRI.create("http://www.bigdata.com/queryHints#optimizer")
  val BigdataRunPriorFirst = bgp(t(Prior, RunFirst, "true" ^^ XSDDatatype.XSDboolean))
  val BigdataAnalyticQuery = t(HintQuery, HintAnalytic, "true" ^^ XSDDatatype.XSDboolean)
  val BigdataNoOptimizer = t(HintQuery, HintOptimizer, "None" ^^ XSDDatatype.XSDstring)
  val `application/sparql-query` = MediaTypes.register(MediaType.custom("application/sparql-query"))
  val `application/sparql-results+xml` = MediaTypes.register(MediaType.custom("application/sparql-results+xml"))
  val `application/rdf+xml` = MediaTypes.register(MediaType.custom("application/rdf+xml"))

  val conf = ConfigFactory.load()
  val KBEndpoint: Uri = Uri(conf.getString("kb-services.kb.endpoint"))
  val Owlery: Uri = Uri(conf.getString("kb-services.owlery.endpoint"))

  val `application/ld+json` = MediaTypes.register(MediaType.custom("application/ld+json"))

  def withOwlery(triple: TripleOrPath): ElementService = service(App.Owlery.toString + "/sparql", bgp(triple))

  def executeSPARQLQuery(query: Query): Future[ResultSet] = sparqlSelectQuery(Post(KBEndpoint, query))

  def executeSPARQLQuery[T](query: Query, resultMapper: QuerySolution => T): Future[Seq[T]] = for {
    resultSet <- sparqlSelectQuery(Post(KBEndpoint, query))
  } yield {
    resultSet.map(resultMapper).toSeq
  }

  def executeSPARQLConstructQuery(query: Query): Future[Model] = sparqlConstructQuery(Post(KBEndpoint, query))

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
  private implicit val RDFXMLUnmarshaller = Unmarshaller.delegate[String, Model](`application/rdf+xml`) { text =>
    val model = ModelFactory.createDefaultModel
    model.read(new ByteArrayInputStream(text.getBytes), null)
    model
  }

  def expandWithOwlet(query: Query): Future[Query] = {
    val pipeline = sendReceive ~> unmarshal[Query]
    pipeline(Post(Owlery.copy(path = Owlery.path / "expand"), query))
  }

  val sparqlSelectQuery: HttpRequest => Future[ResultSet] = addHeader(HttpHeaders.Accept(`application/sparql-results+xml`)) ~> sendReceive ~> unmarshal[ResultSet]
  val sparqlConstructQuery: HttpRequest => Future[Model] = addHeader(HttpHeaders.Accept(`application/rdf+xml`)) ~> sendReceive ~> unmarshal[Model]

}