package org.phenoscape.kb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.log4j.Logger
import org.phenoscape.kb.App.withOwlery
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.semanticweb.owlapi.model.IRI

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution

object PresenceAbsenceOfStructure {

  def statesEntailingAbsence(taxon: IRI, entity: IRI): Future[String] =
    App.executeSPARQLQuery(buildAbsenceQuery(taxon, entity)).map(App.resultSetToTSV(_))

  def statesEntailingPresence(taxon: IRI, entity: IRI): Future[String] =
    App.executeSPARQLQuery(buildPresenceQuery(taxon, entity)).map(App.resultSetToTSV(_))

  def taxaExhibitingPresence(entity: IRI, limit: Int): Future[Seq[Taxon]] = {
    App.executeSPARQLQuery(buildExhibitingPresenceQuery(entity, limit), resultToTaxon)
  }

  def taxaExhibitingAbsence(entity: IRI, limit: Int): Future[Seq[Taxon]] =
    App.executeSPARQLQuery(buildExhibitingAbsenceQuery(entity, limit), resultToTaxon)

  def buildAbsenceQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
    val taxon = Class(taxonIRI)
    val entity = Individual(entityIRI)
    select_distinct('state, 'state_label, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, exhibits_state, 'state),
        t('state, describes_phenotype, 'phenotype),
        t('state, dcDescription, 'state_label),
        t('matrix, has_character, 'matrix_char),
        t('matrix, rdfsLabel, 'matrix_label),
        t('matrix_char, may_have_state_value, 'state)),
        withOwlery(
          t('phenotype, owlEquivalentClass | rdfsSubClassOf, (LacksAllPartsOfType and (towards value entity) and (inheres_in some MultiCellularOrganism)).asOMN)),
          App.BigdataRunPriorFirst)
  }

  def buildPresenceQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
    val taxon = Class(taxonIRI)
    val entity = Class(entityIRI)
    select_distinct('state, 'state_label, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, exhibits_state, 'state),
        t('state, describes_phenotype, 'phenotype),
        t('state, dcDescription, 'state_label),
        t('matrix, has_character, 'matrix_char),
        t('matrix, rdfsLabel, 'matrix_label),
        t('matrix_char, may_have_state_value, 'state)),
        withOwlery(
          t('phenotype, owlEquivalentClass | rdfsSubClassOf, (IMPLIES_PRESENCE_OF some entity).asOMN)),
          App.BigdataRunPriorFirst)
  }

  def buildExhibitingAbsenceQuery(entityIRI: IRI, limit: Int): Query = {
    val query = select_distinct('taxon, 'taxon_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, has_absence_of, entityIRI),
        t('taxon, rdfsLabel, 'taxon_label)))
    query.setLimit(limit)
    query
  }

  def buildExhibitingPresenceQuery(entityIRI: IRI, limit: Int): Query = {
    val entity = Class(entityIRI)
    val query = select_distinct('taxon, 'taxon_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, has_presence_of, entityIRI),
        t('taxon, rdfsLabel, 'taxon_label)))
    query.setLimit(limit)
    query
  }

  private def resultToTaxon(result: QuerySolution): Taxon = Taxon(
    IRI.create(result.getResource("taxon").getURI),
    result.getLiteral("taxon_label").getLexicalForm)

  private lazy val logger = Logger.getLogger(this.getClass)

}