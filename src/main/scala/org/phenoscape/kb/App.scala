package org.phenoscape.kb

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.query.Query
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ResultSetFactory
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.syntax.ElementService
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI

import com.typesafe.config.ConfigFactory

import Main.system
import system.dispatcher
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.HttpCharsets
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.MediaType
import akka.http.scaladsl.model.RequestEntity
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.ActorMaterializer
import akka.util.Timeout

object App {

  implicit val timeout = Timeout(10 minutes)
  implicit val materializer = ActorMaterializer()
  private val Prior = IRI.create("http://www.bigdata.com/queryHints#Prior")
  private val RunFirst = IRI.create("http://www.bigdata.com/queryHints#runFirst")
  private val HintQuery = IRI.create("http://www.bigdata.com/queryHints#Query")
  private val HintAnalytic = IRI.create("http://www.bigdata.com/queryHints#analytic")
  private val HintOptimizer = IRI.create("http://www.bigdata.com/queryHints#optimizer")
  val BigdataRunPriorFirst = bgp(t(Prior, RunFirst, "true" ^^ XSDDatatype.XSDboolean))
  val BigdataAnalyticQuery = t(HintQuery, HintAnalytic, "true" ^^ XSDDatatype.XSDboolean)
  val BigdataNoOptimizer = t(HintQuery, HintOptimizer, "None" ^^ XSDDatatype.XSDstring)
  val `application/sparql-results+xml` = MediaType.applicationWithFixedCharset("sparql-results+xml", HttpCharsets.`UTF-8`, "xml")
  val `application/sparql-query` = MediaType.applicationWithFixedCharset("sparql-query", HttpCharsets.`UTF-8`, "rq", "sparql")
  val `application/rdf+xml` = MediaType.applicationWithFixedCharset("rdf+xml", HttpCharsets.`UTF-8`, "rdf")
  val `application/ld+json` = MediaType.applicationWithFixedCharset("ld+json", HttpCharsets.`UTF-8`, "jsonld")

  val conf = ConfigFactory.load()
  val KBEndpoint: Uri = Uri(conf.getString("kb-services.kb.endpoint"))
  val Owlery: Uri = Uri(conf.getString("kb-services.owlery.endpoint"))

  def withOwlery(triple: TripleOrPath): ElementService = service(App.Owlery.toString + "/sparql", bgp(triple))

  def executeSPARQLQuery(query: Query): Future[ResultSet] = sparqlSelectQuery(query)
  
  def executeSPARQLQuery(queryString: String): Future[ResultSet] = sparqlSelectQuery(queryString)

  def executeSPARQLQuery[T](query: Query, resultMapper: QuerySolution => T): Future[Seq[T]] = for {
    resultSet <- sparqlSelectQuery(query)
  } yield resultSet.asScala.map(resultMapper).toSeq

  def executeSPARQLConstructQuery(query: Query): Future[Model] = sparqlConstructQuery(query)

  def resultSetToTSV(result: ResultSet): String = {
    val outStream = new ByteArrayOutputStream()
    ResultSetFormatter.outputAsTSV(outStream, result)
    val tsv = outStream.toString("utf-8")
    outStream.close()
    tsv
  }

  private implicit val SPARQLQueryMarshaller: ToEntityMarshaller[Query] = Marshaller.stringMarshaller(`application/sparql-query`).compose(_.toString)
  
  private implicit val SPARQLQueryStringMarshaller: ToEntityMarshaller[String] = Marshaller.stringMarshaller(`application/sparql-query`)

  private implicit val SPARQLQueryBodyUnmarshaller: FromEntityUnmarshaller[Query] = Unmarshaller.stringUnmarshaller.forContentTypes(`application/sparql-query`).map(QueryFactory.create)

  private implicit val SPARQLResultsXMLUnmarshaller = Unmarshaller.byteArrayUnmarshaller.forContentTypes(`application/sparql-results+xml`).map { data =>
    // When using the String unmarshaller directly, we don't get fancy characters decoded correctly
    ResultSetFactory.fromXML(new String(data, StandardCharsets.UTF_8))
  }

  private implicit val RDFXMLUnmarshaller = Unmarshaller.byteArrayUnmarshaller.forContentTypes(`application/rdf+xml`).map { data =>
    val model = ModelFactory.createDefaultModel
    model.read(new ByteArrayInputStream(data), null)
    model
  }

  def expandWithOwlet(query: Query): Future[Query] = for {
    requestEntity <- Marshal(query).to[RequestEntity]
    response <- Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      uri = Owlery.copy(path = Owlery.path / "expand"),
      entity = requestEntity))
    newQuery <- Unmarshal(response.entity).to[Query]
  } yield newQuery

  def sparqlSelectQuery(query: Query): Future[ResultSet] = for {
    requestEntity <- Marshal(query).to[RequestEntity]
    response <- Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      headers = List(headers.Accept(`application/sparql-results+xml`)),
      uri = KBEndpoint,
      entity = requestEntity))
    result <- Unmarshal(response.entity).to[ResultSet]
  } yield result
  
  def sparqlSelectQuery(queryString: String): Future[ResultSet] = for {
    requestEntity <- Marshal(queryString).to[RequestEntity]
    response <- Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      headers = List(headers.Accept(`application/sparql-results+xml`)),
      uri = KBEndpoint,
      entity = requestEntity))
    result <- Unmarshal(response.entity).to[ResultSet]
  } yield result

  def sparqlConstructQuery(query: Query): Future[Model] = for {
    requestEntity <- Marshal(query).to[RequestEntity]
    response <- Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      headers = List(headers.Accept(`application/rdf+xml`)),
      uri = KBEndpoint,
      entity = requestEntity))
    model <- Unmarshal(response.entity).to[Model]
  } yield model

}