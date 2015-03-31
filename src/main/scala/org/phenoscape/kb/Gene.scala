package org.phenoscape.kb

import org.phenoscape.kb.Main.system.dispatcher
import scala.collection.JavaConversions._
import scala.concurrent.Future
import org.phenoscape.kb.App.withOwlery
import org.phenoscape.owl.Vocab
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
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct
import com.hp.hpl.jena.sparql.core.Var
import spray.json._
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._
import com.hp.hpl.jena.sparql.expr.E_IsIRI
import com.hp.hpl.jena.graph.NodeFactory

object Gene {

  //FIXME this is a temporary hack until genes are directly associated with taxa in the KB
  val geneIDPrefixToTaxon = Map(
    "http://zfin.org" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_7955"), "zebrafish"),
    "http://www.informatics.jax.org" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_10090"), "mouse"),
    "http://xenbase.org" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_8353"), "frog"),
    "http://www.ncbi.nlm.nih.gov" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_9606"), "human"))

  def search(text: String): Future[Seq[Gene]] = {
    App.executeSPARQLQuery(buildSearchQuery(text), Gene(_))
  }

  def query(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, limit: Int = 20, offset: Int = 0): Future[Seq[Gene]] = for {
    query <- App.expandWithOwlet(buildQuery(entity, taxon, limit, offset))
    descriptions <- App.executeSPARQLQuery(query, Gene(_))
  } yield {
    descriptions
  }

  def queryTotal(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing): Future[ResultCount] = for {
    query <- App.expandWithOwlet(buildTotalQuery(entity, taxon))
    result <- App.executeSPARQLQuery(query)
  } yield {
    ResultCount(result)
  }

  def buildSearchQuery(text: String): Query = {
    val searchText = if (text.endsWith("*")) text else s"$text*"
    val query = select_distinct('gene, 'gene_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('gene_label, BDSearch, NodeFactory.createLiteral(searchText)),
        t('gene_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('gene_label, BDRank, 'rank),
        t('gene, rdfsLabel, 'gene_label),
        t('gene, rdfType, Vocab.Gene)))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(100)
    query
  }

  def buildBasicQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, limit: Int = 20, offset: Int = 0): Query = {
    //TODO allowing expressions makes it impossible to look for absences using the entity as an Individual... fix?
    val entityPatterns = if (entity == owlThing) Nil else
      t('annotation, rdfType, 'phenotype) :: t('phenotype, rdfsSubClassOf, ((has_part some (inheres_in some entity)) or (has_part some (towards some entity))).asOMN) :: Nil
    val taxonPatterns = if (taxon == owlThing) Nil else
      t('annotation, associated_with_taxon, 'taxon) :: t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('annotation, rdfType, AnnotatedPhenotype) ::
          t('annotation, associated_with_gene, 'gene) ::
          t('gene, rdfsLabel, 'gene_label) ::
          taxonPatterns ++
          entityPatterns: _*))
    query
  }

  def buildQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, limit: Int = 20, offset: Int = 0): Query = {
    val query = buildBasicQuery(entity, taxon)
    query.addResultVar('gene)
    query.addResultVar('gene_label)
    query.setOffset(offset)
    if (limit > 0) query.setLimit(limit)
    query.addOrderBy('gene_label)
    query.addOrderBy('gene)
    query
  }

  def buildTotalQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing): Query = {
    val query = buildBasicQuery(entity, taxon)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("gene"))))
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

  def apply(result: QuerySolution): Gene = {
    val geneURI = result.getResource("gene").getURI
    val taxon = geneIDPrefixToTaxon.collectFirst {
      case (prefix, taxon) if geneURI.startsWith(prefix) => taxon
    }.getOrElse(Taxon(factory.getOWLThing.getIRI, "unknown"))
    Gene(
      IRI.create(geneURI),
      result.getLiteral("gene_label").getLexicalForm,
      taxon)
  }

}

case class Gene(iri: IRI, label: String, taxon: Taxon) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString.toJson, "label" -> label.toJson, "taxon" -> taxon.toJSON).toJson.asJsObject
  }

}