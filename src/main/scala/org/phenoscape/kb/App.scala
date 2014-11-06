package org.phenoscape.kb

import scala.concurrent.ExecutionContext.Implicits.global
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
import spray.http.MediaTypes
import spray.http.MediaType
import com.hp.hpl.jena.query.ResultSetFormatter
import java.io.ByteArrayOutputStream
import spray.json.JsValue
import spray.json.JsObject

object App {

  private val Prior = IRI.create("http://www.bigdata.com/queryHints#Prior")
  private val RunFirst = IRI.create("http://www.bigdata.com/queryHints#runFirst")
  val BigdataRunPriorFirst = bgp(t(Prior, RunFirst, "true" ^^ XSDDatatype.XSDboolean))

  val conf = ConfigFactory.load()
  val KBEndpoint = IRI.create(conf.getString("kb-services.kb.endpoint"))
  val OwleryEndpoint = IRI.create(conf.getString("kb-services.owlery.endpoint"))

  val `application/ld+json` = MediaTypes.register(MediaType.custom("application/ld+json"))

  def withOwlery(triple: TriplePath): ElementService = service(App.OwleryEndpoint.toString, bgp(triple))

  def executeSPARQLQuery(query: Query): Future[ResultSet] = Future {
    blocking {
      val queryEngine = new QueryEngineHTTP(App.KBEndpoint.toString, query)
      val resultSet = ResultSetFactory.copyResults(queryEngine.execSelect)
      queryEngine.close()
      resultSet
    }
  }

  def executeSPARQLQuery[T](query: Query, resultMapper: QuerySolution => T): Future[Seq[T]] = Future {
    blocking {
      val queryEngine = new QueryEngineHTTP(App.KBEndpoint.toString, query)
      val resultSet = queryEngine.execSelect
      val results = resultSet.map(resultMapper).toVector
      queryEngine.close()
      results
    }
  }

  def resultSetToTSV(result: ResultSet): String = {
    val outStream = new ByteArrayOutputStream()
    ResultSetFormatter.outputAsTSV(outStream, result)
    val tsv = outStream.toString("utf-8")
    outStream.close()
    tsv
  }

}