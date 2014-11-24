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

  def search(text: String, termType: IRI, property: IRI): Future[Seq[MinimalTerm]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, termType, property), Term.fromMinimalQuerySolution)
  }

  def searchAnatomicalTerms(text: String, limit: Int): Future[Seq[MinimalTerm]] = {
    App.executeSPARQLQuery(buildAnatomicalTermQuery(text, limit), Term.fromMinimalQuerySolution)
  }

  def label(iri: IRI): Future[Option[MinimalTerm]] = {
    def convert(result: QuerySolution): MinimalTerm = MinimalTerm(iri, result.getLiteral("term_label").getLexicalForm)
    App.executeSPARQLQuery(buildLabelQuery(iri), convert).map(_.headOption)
  }

  def labels(iris: IRI*): Future[Seq[MinimalTerm]] = {
    App.executeSPARQLQuery(buildLabelsQuery(iris: _*), Term.fromMinimalQuerySolution)
  }

  def withIRI(iri: IRI): Future[Option[Term]] = {
    App.executeSPARQLQuery(buildTermQuery(iri), Term.fromQuerySolution(_)(iri)).map(_.headOption)

  }

  def buildTermQuery(iri: IRI): Query = {
    select('label, 'definition) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'label)),
        optional(
          bgp(
            t(iri, definition, 'definition))))
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

  def fromQuerySolution(result: QuerySolution)(iri: IRI): Term = Term(iri,
    result.getLiteral("label").getLexicalForm,
    Option(result.getLiteral("definition")).map(_.getLexicalForm).getOrElse(""))

  def fromQuerySolution2(iri: IRI)(result: QuerySolution): Term = Term(iri,
    result.getLiteral("label").getLexicalForm,
    Option(result.getLiteral("definition")).map(_.getLexicalForm).getOrElse(""))

  def fromMinimalQuerySolution(result: QuerySolution): MinimalTerm = MinimalTerm(
    IRI.create(result.getResource("term").getURI),
    result.getLiteral("term_label").getLexicalForm)

}

case class Term(iri: IRI, label: String, definition: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString, "label" -> label, "definition" -> definition).toJson.asJsObject
  }

}

case class MinimalTerm(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject
  }

}
