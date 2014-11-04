package org.phenoscape.kb

import scala.concurrent.Future

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

object Term {

  private val BDSearchPrefix = "http://www.bigdata.com/rdf/search#"
  private val BDSearch = IRI.create(s"${BDSearchPrefix}search")
  private val BDMatchAllTerms = IRI.create(s"${BDSearchPrefix}matchAllTerms")
  private val BDRank = IRI.create(s"${BDSearchPrefix}rank")
  private val rdfsLabel = ObjectProperty(RDFS_LABEL.getIRI)
  private val hasExactSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasExactSynonym")
  private val hasRelatedSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym")
  private val rdfsIsDefinedBy = ObjectProperty(RDFS.isDefinedBy.getURI)
  private val owlClass = IRI.create(OWL2.Class.getURI)
  private val Uberon = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")

  def search(text: String, termType: IRI, property: IRI): Future[Seq[TermSearchResult]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, termType, property), TermSearchResult.fromQuerySolution)
  }

  def searchAnatomicalTerms(text: String, limit: Int): Future[Seq[TermSearchResult]] = {
    App.executeSPARQLQuery(buildAnatomicalTermQuery(text, limit), TermSearchResult.fromQuerySolution)
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

  private def buildAnatomicalTermQuery(text: String, limit: Int): Query = {
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

  implicit val TermSearchResultsMarshaller = Marshaller.delegate[Seq[TermSearchResult], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

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