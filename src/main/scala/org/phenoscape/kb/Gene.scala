package org.phenoscape.kb

import scala.concurrent.Future

import org.phenoscape.kb.App.withOwlery
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI

import com.hp.hpl.jena.query.Query

object Gene {

  def affectingPhenotype(entity: IRI, quality: IRI): Future[Seq[String]] = {
    App.executeSPARQLQuery(buildGeneForPhenotypeQuery(entity, quality), _.toString)
  }

  private def buildGeneForPhenotypeQuery(entityIRI: IRI, qualityIRI: IRI): Query = {
    val quality = Class(qualityIRI)
    select_distinct('gene, 'gene_label, 'taxon_label, 'source) from "http://kb.phenoscape.org/" where (
      bgp(
        t('pheno_instance, rdfType, 'phenotype),
        t('pheno_instance, associated_with_taxon, 'taxon),
        t('taxon, rdfsLabel, 'taxon_label),
        t('pheno_instance, associated_with_gene, 'gene),
        t('gene, rdfsLabel, 'gene_label)),
        optional(bgp(
          t('pheno_instance, dcSource, 'source))),
        withOwlery(
          t('phenotype, rdfsSubClassOf, ((has_part some (quality and (inheres_in_part_of some Class(entityIRI)))) or (has_part some (quality and (TOWARDS some Class(entityIRI)))) or (has_part some (quality and (TOWARDS value Individual(entityIRI))))).asOMN)),
          App.BigdataRunPriorFirst)
  }

  def expressedWithinStructure(entity: IRI): Future[Seq[String]] = {
    App.executeSPARQLQuery(buildExpressionQuery(entity), _.toString)
  }

  private def buildExpressionQuery(entityIRI: IRI): Query = {
    val entity = Class(entityIRI)
    select_distinct('gene, 'gene_label, 'taxon_label, 'source) from "http://kb.phenoscape.org/" where (
      bgp(
        t('structure, rdfType, 'entity),
        t('expression, OCCURS_IN, 'structure),
        t('expression, rdfType, GeneExpression),
        t('expression, associated_with_taxon, 'taxon),
        t('taxon, rdfsLabel, 'taxon_label),
        t('expression, associated_with_gene, 'gene),
        t('gene, rdfsLabel, 'gene_label)),
        optional(bgp(
          t('expression, dcSource, 'source))),
        withOwlery(
          t('entity, rdfsSubClassOf, (part_of some entity).asOMN)),
          App.BigdataRunPriorFirst)
  }

}