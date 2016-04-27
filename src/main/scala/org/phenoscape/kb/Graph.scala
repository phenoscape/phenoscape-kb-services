package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import Main.system
import system.dispatcher
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI

import com.hp.hpl.jena.query.Query

import spray.client.pipelining._
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json._
import spray.json.DefaultJsonProtocol._

object Graph {

  def propertyNeighborsForObject(term: IRI, property: IRI): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQueryObject(term, property), Term.fromMinimalQuerySolution)

  def propertyNeighborsForSubject(term: IRI, property: IRI): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQuerySubject(term, property), Term.fromMinimalQuerySolution)

  private def buildPropertyNeighborsQueryObject(focalTerm: IRI, property: IRI): Query = {
    val classRelation = NamedRestrictionGenerator.getClassRelationIRI(property)
    select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('existential_node, classRelation, focalTerm),
        t('existential_subclass, rdfsSubClassOf, 'existential_node),
        t('existential_subclass, classRelation, 'term),
        t('term, rdfsLabel, 'term_label)))
  }

  private def buildPropertyNeighborsQuerySubject(focalTerm: IRI, property: IRI): Query = {
    val classRelation = NamedRestrictionGenerator.getClassRelationIRI(property)
    select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('existential_node, classRelation, focalTerm),
        t('existential_node, rdfsSubClassOf, 'existential_superclass),
        t('existential_superclass, classRelation, 'term),
        t('term, rdfsLabel, 'term_label)))
  }

}