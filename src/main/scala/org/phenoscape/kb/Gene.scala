package org.phenoscape.kb

import org.phenoscape.kb.Main.system.dispatcher
import scala.collection.JavaConversions._
import scala.concurrent.Future
import org.phenoscape.kb.App.withOwlery
import org.phenoscape.owl.Vocab._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import com.hp.hpl.jena.sparql.expr.E_OneOf
import org.semanticweb.owlapi.model.OWLClassExpression
import com.hp.hpl.jena.sparql.syntax.ElementFilter

object Gene {

  def buildQuery(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, limit: Int = 20, offset: Int = 0): Query = {
    val entityPatterns = if (entity == OWLThing) Nil else
      t('phenotype, 'pred, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, describes_phenotype, 'phenotype) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('taxon, exhibits_state, 'state) ::
          t('taxon, rdfsLabel, 'taxon_label) ::
          entityPatterns: _*))

    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    query.setOffset(offset)
    query.setLimit(limit)
    query.addOrderBy('taxon_label)
    query.addOrderBy('taxon)
    query
  }

  def affectingPhenotype(entity: IRI, quality: IRI): Future[String] = {
    val header = "gene\tgeneLabel\ttaxon\tsource\n"
    val result = App.executeSPARQLQuery(buildGeneForPhenotypeQuery(entity, quality), formatResult)
    result.map(header + _.mkString("\n"))
  }

  private def formatResult(result: QuerySolution): String = {
    val gene = result.getResource("gene").getURI
    val geneLabel = result.getLiteral("gene_label").getLexicalForm
    val taxon = result.getLiteral("taxon_label").getLexicalForm
    val source = Option(result.getResource("source")).map(_.getURI).getOrElse("")
    s"$gene\t$geneLabel\t$taxon\t$source"
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
          t('phenotype, rdfsSubClassOf, ((has_part some (quality and (inheres_in_part_of some Class(entityIRI)))) or (has_part some (quality and (towards some Class(entityIRI)))) or (has_part some (quality and (towards value Individual(entityIRI))))).asOMN)),
          App.BigdataRunPriorFirst)
  }

  def expressedWithinStructure(entity: IRI): Future[String] = {
    val header = "gene\tgeneLabel\ttaxon\tsource\n"
    val result = App.executeSPARQLQuery(buildExpressionQuery(entity), formatResult)
    result.map(header + _.mkString("\n"))
  }

  private def buildExpressionQuery(entityIRI: IRI): Query = {
    val entity = Class(entityIRI)
    select_distinct('gene, 'gene_label, 'taxon_label, 'source) from "http://kb.phenoscape.org/" where (
      bgp(
        t('structure, rdfType, 'entity),
        t('expression, occurs_in, 'structure),
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

case class Gene(iri: IRI, label: String)