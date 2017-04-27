package org.phenoscape.kb

import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.query.Query
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI

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