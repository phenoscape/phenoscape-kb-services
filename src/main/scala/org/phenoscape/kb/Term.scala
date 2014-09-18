package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owl.Vocab._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_LABEL
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDF_TYPE
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.expr.E_IsIRI
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import spray.http._
import spray.httpx.marshalling._
import spray.httpx.SprayJsonSupport._
import scala.util.parsing.json.JSONObject
import org.phenoscape.kb.App.`application/ld+json`

import spray.json.DefaultJsonProtocol._
import spray.json._

object Term {

  private val BDSearchPrefix = "http://www.bigdata.com/rdf/search#"
  private val BDSearch = IRI.create(s"${BDSearchPrefix}search")
  private val BDMatchAllTerms = IRI.create(s"${BDSearchPrefix}matchAllTerms")
  private val BDRank = IRI.create(s"${BDSearchPrefix}rank")

  def search(text: String, termType: IRI, property: IRI): Future[Seq[TermSearchResult]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, termType, property), TermSearchResult.fromQuerySolution)
  }

  private def buildSearchQuery(text: String, termType: IRI, property: IRI): Query = {
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('term_label, BDSearch, NodeFactory.createLiteral(text)),
        t('term_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('term_label, BDRank, 'rank),
        t('term, rdfsLabel, 'term_label),
        t('term, rdfType, termType)),
        new ElementFilter((new E_IsIRI(new ExprVar('term)))))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(100)
    query
  }

  case class TermSearchResult(iri: IRI, label: String) {
    def toJSON: JsValue = {
      Map("@id" -> iri.toString, "label" -> label).toJson
    }
  }

  object TermSearchResult {

    def fromQuerySolution(result: QuerySolution): TermSearchResult = TermSearchResult(
      IRI.create(result.getResource("term").getURI),
      result.getLiteral("term_label").getLexicalForm)

  }

  implicit val TermSearchResultsMarshaller = Marshaller.delegate[Seq[TermSearchResult], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

}

