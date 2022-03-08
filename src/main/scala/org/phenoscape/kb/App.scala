package org.phenoscape.kb

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.{Marshal, Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal, Unmarshaller}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.ResultSetMgr
import org.apache.jena.riot.resultset.ResultSetLang
import org.apache.jena.sparql.syntax.ElementService
import org.phenoscape.kb.Main.system
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.sparql.FromQuerySolution
import org.semanticweb.owlapi.model.IRI

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import org.phenoscape.sparql.FromQuerySolution.mapSolution
import org.phenoscape.sparql.FromQuerySolutionOWL._

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

  val `application/sparql-results+xml` =
    MediaType.applicationWithFixedCharset("sparql-results+xml", HttpCharsets.`UTF-8`, "xml")

  val `application/sparql-query` =
    MediaType.applicationWithFixedCharset("sparql-query", HttpCharsets.`UTF-8`, "rq", "sparql")

  val `application/rdf+xml` = MediaType.applicationWithFixedCharset("rdf+xml", HttpCharsets.`UTF-8`, "rdf")
  val `application/ld+json` = MediaType.applicationWithFixedCharset("ld+json", HttpCharsets.`UTF-8`, "jsonld")

  val conf = ConfigFactory.load()
  val basePath = conf.getString("kb-services.base-path")
  val KBEndpoint: Uri = Uri(conf.getString("kb-services.kb.endpoint"))
  val Owlery: Uri = Uri(conf.getString("kb-services.owlery.endpoint"))

  def withOwlery(triple: TripleOrPath): ElementService = service(App.Owlery.toString + "/sparql", bgp(triple))

  def executeSPARQLQuery(query: Query): Future[ResultSet] = sparqlSelectQuery(query)

  def executeSPARQLQuery(queryString: String): Future[ResultSet] = sparqlSelectQuery(queryString)

  def executeSPARQLQuery[T](query: Query, resultMapper: QuerySolution => T): Future[Seq[T]] =
    for {
      resultSet <- sparqlSelectQuery(query)
    } yield resultSet.asScala.map(resultMapper).toSeq

  def executeSPARQLQueryCase[T: FromQuerySolution](query: Query): Future[Seq[T]] =
    executeSPARQLQueryStringCase[T](query.toString)

  def executeSPARQLQueryStringCase[T: FromQuerySolution](query: String): Future[Seq[T]] =
    for {
      resultSet    <- sparqlSelectQuery(query)
      results       = resultSet.asScala.map(mapSolution[T])
      asFutures     = results.map(Future.fromTry)
      validResults <- Future.sequence(asFutures)
    } yield validResults.toSeq

  def executeSPARQLQueryString[T](queryString: String, resultMapper: QuerySolution => T): Future[Seq[T]] =
    for {
      resultSet <- sparqlSelectQuery(queryString)
    } yield resultSet.asScala.map(resultMapper).toSeq

  def executeSPARQLConstructQuery(query: Query): Future[Model] = sparqlConstructQuery(query)

  def executeSPARQLAskQuery(query: Query): Future[Boolean] = sparqlAskQuery(query)

  def resultSetToTSV(result: ResultSet): String = {
    val outStream = new ByteArrayOutputStream()
    ResultSetFormatter.outputAsTSV(outStream, result)
    val tsv = outStream.toString("utf-8")
    outStream.close()
    tsv
  }

  implicit private val SPARQLQueryMarshaller: ToEntityMarshaller[Query] =
    Marshaller.stringMarshaller(`application/sparql-query`).compose(_.toString)

  implicit private val SPARQLQueryStringMarshaller: ToEntityMarshaller[String] =
    Marshaller.stringMarshaller(`application/sparql-query`)

  implicit private val SPARQLQueryBodyUnmarshaller: FromEntityUnmarshaller[Query] =
    Unmarshaller.stringUnmarshaller.forContentTypes(`application/sparql-query`).map(QueryFactory.create)

  implicit private val SPARQLResultsXMLUnmarshaller =
    Unmarshaller.byteArrayUnmarshaller.forContentTypes(`application/sparql-results+xml`).map { data =>
      // When using the String unmarshaller directly, we don't get fancy characters decoded correctly
      val inputStream = new ByteArrayInputStream(data)
      val result = ResultSetMgr.read(inputStream, ResultSetLang.SPARQLResultSetXML)
      inputStream.close()
      result
    }

  implicit private val SPARQLResultsBooleanUnmarshaller =
    Unmarshaller.byteArrayUnmarshaller.forContentTypes(`application/sparql-results+xml`).map { data =>
      // When using the String unmarshaller directly, we don't get fancy characters decoded correctly
      val inputStream = new ByteArrayInputStream(data)
      val result = ResultSetMgr.readBoolean(inputStream, ResultSetLang.SPARQLResultSetXML)
      inputStream.close()
      result
    }

  implicit private val RDFXMLUnmarshaller =
    Unmarshaller.byteArrayUnmarshaller.forContentTypes(`application/rdf+xml`).map { data =>
      val model = ModelFactory.createDefaultModel
      model.read(new ByteArrayInputStream(data), null)
      model
    }

  def expandWithOwlet(query: Query): Future[Query] =
    for {
      requestEntity <- Marshal(query).to[RequestEntity]
      response <- Http().singleRequest(
        HttpRequest(method = HttpMethods.POST,
                    uri = Owlery.copy(path = Owlery.path / "expand"),
                    entity = requestEntity))
      newQuery <- Unmarshal(response.entity).to[Query]
    } yield newQuery

  def sparqlSelectQuery(query: Query): Future[ResultSet] =
    for {
      requestEntity <- Marshal(query).to[RequestEntity]
      response <- Http().singleRequest(
        HttpRequest(method = HttpMethods.POST,
                    headers = List(headers.Accept(`application/sparql-results+xml`)),
                    uri = KBEndpoint,
                    entity = requestEntity))
      result <- Unmarshal(response.entity).to[ResultSet]
    } yield result

  def sparqlSelectQuery(queryString: String): Future[ResultSet] =
    for {
      requestEntity <- Marshal(queryString).to[RequestEntity]
      response <- Http().singleRequest(
        HttpRequest(method = HttpMethods.POST,
                    headers = List(headers.Accept(`application/sparql-results+xml`)),
                    uri = KBEndpoint,
                    entity = requestEntity))
      result <- Unmarshal(response.entity).to[ResultSet]
    } yield result

  def sparqlConstructQuery(query: Query): Future[Model] =
    for {
      requestEntity <- Marshal(query).to[RequestEntity]
      response <- Http().singleRequest(
        HttpRequest(method = HttpMethods.POST,
                    headers = List(headers.Accept(`application/rdf+xml`)),
                    uri = KBEndpoint,
                    entity = requestEntity))
      model <- Unmarshal(response.entity).to[Model]
    } yield model

  def sparqlAskQuery(query: Query): Future[Boolean] =
    for {
      requestEntity <- Marshal(query).to[RequestEntity]
      response <- Http().singleRequest(
        HttpRequest(method = HttpMethods.POST,
                    headers = List(headers.Accept(`application/sparql-results+xml`)),
                    uri = KBEndpoint,
                    entity = requestEntity))
      result <- Unmarshal(response.entity).to[Boolean]
    } yield result

}
