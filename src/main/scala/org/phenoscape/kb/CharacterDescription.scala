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
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.E_OneOf
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel

object CharacterDescription {

  def search(text: String, limit: Int): Future[Seq[CharacterDescription]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, limit), CharacterDescription(_))
  }

  def buildSearchQuery(text: String, limit: Int): Query = {
    val query = select_distinct('state, 'state_desc) from "http://kb.phenoscape.org/" where (
      bgp(
        t('state_desc, BDSearch, NodeFactory.createLiteral(text)),
        t('state_desc, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('state_desc, BDRank, 'rank),
        t('state, dcDescription, 'state_desc),
        t('state, rdfType, StandardState)))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(limit)
    query
  }

  def apply(result: QuerySolution): CharacterDescription = CharacterDescription(
    IRI.create(result.getResource("state").getURI),
    result.getLiteral("state_desc").getLexicalForm)

  implicit val CharacterDescriptionsMarshaller = Marshaller.delegate[Seq[CharacterDescription], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

}

case class CharacterDescription(iri: IRI, description: String) {

  def toJSON: JsValue = {
    Map("@id" -> iri.toString, "label" -> description).toJson
  }

}