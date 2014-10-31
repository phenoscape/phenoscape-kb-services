package org.phenoscape.kb

import scala.concurrent.Future

import org.apache.log4j.Logger
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.kb.App.withOwlery

import com.hp.hpl.jena.query.Query

object PresenceAbsenceOfStructure {

  def statesEntailingAbsence(taxon: IRI, entity: IRI): Future[Seq[String]] =
    App.executeSPARQLQuery(buildAbsenceQuery(taxon, entity), _.toString)

  def statesEntailingPresence(taxon: IRI, entity: IRI): Future[Seq[String]] =
    App.executeSPARQLQuery(buildPresenceQuery(taxon, entity), _.toString)

  def taxaExhibitingPresence(entity: IRI): Future[Seq[String]] = {
    App.executeSPARQLQuery(buildExhibitingPresenceQuery(entity), _.toString)
  }

  def taxaExhibitingAbsence(entity: IRI): Future[Seq[String]] =
    App.executeSPARQLQuery(buildExhibitingAbsenceQuery(entity), _.toString)

  private def buildAbsenceQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
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
          t('phenotype, rdfsSubClassOf, (LacksAllPartsOfType and (towards value entity) and (inheres_in some MultiCellularOrganism)).asOMN)),
          App.BigdataRunPriorFirst)
  }

  private def buildPresenceQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
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
          t('phenotype, rdfsSubClassOf, (IMPLIES_PRESENCE_OF some entity).asOMN)),
          App.BigdataRunPriorFirst)
  }

  private def buildExhibitingAbsenceQuery(entityIRI: IRI): Query = {
    val entity = Individual(entityIRI)
    select_distinct('taxon, 'taxon_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, exhibits_state / describes_phenotype, 'phenotype),
        t('taxon, rdfsLabel, 'taxon_label)),
        withOwlery(
          t('phenotype, rdfsSubClassOf, (LacksAllPartsOfType and (towards value entity) and (inheres_in some MultiCellularOrganism)).asOMN)),
          App.BigdataRunPriorFirst)
  }

  private def buildExhibitingPresenceQuery(entityIRI: IRI): Query = {
    val entity = Class(entityIRI)
    select_distinct('taxon, 'taxon_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, exhibits_state / describes_phenotype, 'phenotype),
        t('taxon, rdfsLabel, 'taxon_label)),
        withOwlery(
          t('phenotype, rdfsSubClassOf, (IMPLIES_PRESENCE_OF some entity).asOMN)),
          App.BigdataRunPriorFirst)
  }

  lazy val logger = Logger.getLogger(this.getClass)

}