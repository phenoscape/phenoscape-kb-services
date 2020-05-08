package org.phenoscape.kb.util

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.{Marshal, Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity, headers}
import akka.stream.alpakka.xml._
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.scaladsl.Source
import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.query.{Query, QuerySolution, QuerySolutionMap}
import org.apache.jena.rdf.model.impl.ResourceImpl
import org.apache.jena.rdf.model.{AnonId, ResourceFactory}
import org.phenoscape.kb.App.{KBEndpoint, `application/sparql-query`, `application/sparql-results+xml`}
import org.phenoscape.kb.Main.system
import org.phenoscape.kb.Main.system.dispatcher

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

object StreamingSPARQLResults {

  private implicit val SPARQLQueryMarshaller: ToEntityMarshaller[Query] = Marshaller.stringMarshaller(`application/sparql-query`).compose(_.toString)

  private sealed trait LiteralType extends Product with Serializable

  private case object Plain extends LiteralType

  private final case class Lang(lang: String) extends LiteralType

  private final case class DataType(uri: String) extends LiteralType

  private implicit val SPARQLQueryStringMarshaller: ToEntityMarshaller[String] = Marshaller.stringMarshaller(`application/sparql-query`)

  def streamSelectQuery(futureQuery: Future[String]): Source[QuerySolution, NotUsed] = {
    val reqFuture = futureQuery.flatMap(Marshal(_).to[RequestEntity])
    Source.future(reqFuture)
      .map(req => HttpRequest(
        method = HttpMethods.POST,
        headers = List(headers.Accept(`application/sparql-results+xml`)),
        uri = KBEndpoint,
        entity = req) -> NotUsed)
      .via(Http().superPool())
      .map(_._1)
      .flatMapConcat {
        case Success(response) => response.entity.dataBytes
        case Failure(error)    => ???
      }
      .via(XmlParsing.parser)
      .statefulMapConcat { () => {
        // state
        var qs = new QuerySolutionMap()
        var currentVariable = ""
        var currentLiteralType: LiteralType = Plain
        val valueBuffer = StringBuilder.newBuilder
        // aggregation function
        parseEvent =>
          parseEvent match {
            case s: StartElement if s.localName == "result"  =>
              qs = new QuerySolutionMap()
              currentVariable = ""
              immutable.Seq.empty
            case s: StartElement if s.localName == "binding" =>
              currentVariable = s.attributes("name")
              immutable.Seq.empty
            case s: StartElement if s.localName == "uri"     =>
              valueBuffer.clear()
              immutable.Seq.empty
            case s: EndElement if s.localName == "uri"       =>
              val value = ResourceFactory.createResource(valueBuffer.toString)
              qs.add(currentVariable, value)
              immutable.Seq.empty
            case s: StartElement if s.localName == "bnode"   =>
              valueBuffer.clear()
              immutable.Seq.empty
            case s: EndElement if s.localName == "bnode"     =>
              val value = new ResourceImpl(new AnonId(valueBuffer.toString))
              qs.add(currentVariable, value)
              immutable.Seq.empty
            case s: StartElement if s.localName == "literal" =>
              valueBuffer.clear()
              currentLiteralType = s.attributes.get("datatype").map(DataType).orElse(s.attributes.get("xml:lang").map(Lang)).getOrElse(Plain)
              immutable.Seq.empty
            case s: EndElement if s.localName == "literal"   =>
              val text = valueBuffer.toString
              val literal = currentLiteralType match {
                case Plain         => ResourceFactory.createPlainLiteral(text)
                case Lang(lang)    => ResourceFactory.createLangLiteral(text, lang)
                case DataType(uri) =>
                  val datatype = TypeMapper.getInstance().getSafeTypeByName(uri)
                  ResourceFactory.createTypedLiteral(text, datatype)
              }
              qs.add(currentVariable, literal)
              immutable.Seq.empty
            case s: EndElement if s.localName == "result"    =>
              immutable.Seq(qs)
            case t: TextEvent                                =>
              valueBuffer.append(t.text)
              immutable.Seq.empty
            case _                                           =>
              immutable.Seq.empty
          }
      }
      }
  }

  def streamSelectQuery(query: String): Source[QuerySolution, NotUsed] = streamSelectQuery(Future.successful(query))

}
