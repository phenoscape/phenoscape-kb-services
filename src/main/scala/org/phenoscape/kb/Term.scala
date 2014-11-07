package org.phenoscape.kb

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_LABEL
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.expr.E_IsIRI
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.vocabulary.OWL2
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._
import spray.json._
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.E_OneOf
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel

object Term {

  def search(text: String, termType: IRI, property: IRI): Future[Seq[TermSearchResult]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, termType, property), TermSearchResult.fromQuerySolution)
  }

  def searchAnatomicalTerms(text: String, limit: Int): Future[Seq[TermSearchResult]] = {
    App.executeSPARQLQuery(buildAnatomicalTermQuery(text, limit), TermSearchResult.fromQuerySolution)
  }

  def label(iri: IRI): Future[Option[TermSearchResult]] = {
    def convert(result: QuerySolution): TermSearchResult = TermSearchResult(iri, result.getLiteral("term_label").getLexicalForm)
    App.executeSPARQLQuery(buildLabelQuery(iri), convert).map(_.headOption)
  }

  def labels(iris: IRI*): Future[Seq[TermSearchResult]] = {
    App.executeSPARQLQuery(buildLabelsQuery(iris: _*), TermSearchResult.fromQuerySolution)
  }

  def buildSearchQuery(text: String, termType: IRI, property: IRI): Query = {
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

  def buildAnatomicalTermQuery(text: String, limit: Int): Query = {
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('matched_label, BDSearch, NodeFactory.createLiteral(text)),
        t('matched_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('matched_label, BDRank, 'rank),
        t('term, rdfsLabel | (hasExactSynonym | hasRelatedSynonym), 'matched_label),
        t('term, rdfsLabel, 'term_label),
        t('term, rdfsIsDefinedBy, Uberon),
        t('term, rdfType, owlClass)),
        new ElementFilter((new E_IsIRI(new ExprVar('term)))))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(limit)
    query
  }

  def buildLabelQuery(iri: IRI): Query = {
    val query = select('term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'term_label)))
    query.setLimit(1)
    query
  }

  def buildLabelsQuery(iris: IRI*): Query = {
    val nodes = iris.map(iri => new NodeValueNode(NodeFactory.createURI(iri.toString)))
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('term, rdfsLabel, 'term_label)),
        new ElementFilter(new E_OneOf(new ExprVar('term), new ExprList(nodes))))
    query
  }

  implicit val JSONResultItemsMarshaller = Marshaller.delegate[Seq[JSONResultItem], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

  implicit val JSONResultItemMarshaller = Marshaller.delegate[JSONResultItem, JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { result =>
    result.toJSON
  }

  implicit val IRIMarshaller = Marshaller.delegate[IRI, JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { iri =>
    new JsObject(Map("@id" -> iri.toString.toJson))
  }

}

case class TermSearchResult(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject
  }

}

object TermSearchResult {

  def fromQuerySolution(result: QuerySolution): TermSearchResult = TermSearchResult(
    IRI.create(result.getResource("term").getURI),
    result.getLiteral("term_label").getLexicalForm)

}