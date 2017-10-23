package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.SortCondition
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_NotOneOf
import org.apache.jena.sparql.expr.E_StrLowerCase
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.syntax.ElementFilter
import org.apache.jena.sparql.syntax.ElementSubQuery
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClassExpression

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes
import spray.json._
import spray.json.DefaultJsonProtocol._

case class GenePhenotypeAnnotation(gene: MinimalTerm, phenotype: MinimalTerm, source: Option[IRI]) extends JSONResultItem {

  def toJSON: JsObject = {
    (Map("gene" -> gene.toJSON,
      "phenotype" -> phenotype.toJSON,
      "source" -> source.map(_.toString).getOrElse("").toJson)).toJson.asJsObject
  }

  override def toString(): String = {
    s"${gene.iri}\t${gene.label}\t${phenotype.iri}\t${phenotype.label}\t${source}"
  }

}

object GenePhenotypeAnnotation {

  def queryAnnotations(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Seq[GenePhenotypeAnnotation]] = for {
    query <- buildGenePhenotypeAnnotationsQuery(entity, quality, inTaxonOpt, limit, offset)
    annotations <- App.executeSPARQLQuery(query, fromQueryResult)
  } yield annotations

  def queryAnnotationsTotal(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Int] = for {
    query <- buildGenePhenotypeAnnotationsTotalQuery(entity, quality, inTaxonOpt)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def fromQueryResult(result: QuerySolution): GenePhenotypeAnnotation = GenePhenotypeAnnotation(
    MinimalTerm(IRI.create(result.getResource("gene").getURI),
      result.getLiteral("gene_label").getLexicalForm),
    MinimalTerm(IRI.create(result.getResource("phenotype").getURI),
      result.getLiteral("phenotype_label").getLexicalForm),
    Option(result.getResource("source")).map(v => IRI.create(v.getURI)))

  private def buildBasicGenePhenotypeAnnotationsQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
    val phenotypeExpression = (entity, quality) match {
      case (Some(entityTerm), Some(qualityTerm)) => Some((has_part some qualityTerm) and (phenotype_of some entityTerm))
      case (Some(entityTerm), None)              => Some(phenotype_of some entityTerm)
      case (None, Some(qualityTerm))             => Some(has_part some qualityTerm)
      case (None, None)                          => None
    }
    val phenotypeTriple = phenotypeExpression.map(desc => t('phenotype, rdfsSubClassOf, desc.asOMN)).toList
    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf*, _)).toList
    val query = select_distinct('gene, 'gene_label, 'phenotype, 'phenotype_label, 'source) where (
      bgp(
        App.BigdataAnalyticQuery ::
          t('annotation, rdfType, AnnotatedPhenotype) ::
          t('annotation, associated_with_gene, 'gene) ::
          t('gene, rdfsLabel, 'gene_label) ::
          t('annotation, rdfType, 'phenotype) ::
          t('phenotype, rdfsLabel, 'phenotype_label) ::
          t('annotation, associated_with_taxon, 'taxon) ::
          phenotypeTriple ++
          taxonPatterns: _*),
        optional(bgp(t('annotation, dcSource, 'source))),
        new ElementFilter(new E_NotOneOf(new ExprVar('phenotype), new ExprList(List(
          new NodeValueNode(AnnotatedPhenotype),
          new NodeValueNode(owlNamedIndividual))))))
    App.expandWithOwlet(query)
  }

  def buildGenePhenotypeAnnotationsQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Query] = {
    for {
      rawQuery <- buildBasicGenePhenotypeAnnotationsQuery(entity, quality, inTaxonOpt)
    } yield {
      val query = rawQuery from KBMainGraph.toString
      query.setOffset(offset)
      if (limit > 0) query.setLimit(limit)
      query.addOrderBy(new SortCondition(new E_StrLowerCase(new NodeValueNode(NodeFactory.createVariable("gene_label"))), Query.ORDER_DEFAULT))
      query.addOrderBy('gene)
      query.addOrderBy('phenotype_label)
      query.addOrderBy('phenotype)
      query
    }
  }

  def buildGenePhenotypeAnnotationsTotalQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
    for {
      rawQuery <- buildBasicGenePhenotypeAnnotationsQuery(entity, quality, inTaxonOpt)
    } yield {
      val query = select() from KBMainGraph.toString where (new ElementSubQuery(rawQuery))
      query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
      query
    }
  }

  val AnnotationTextMarshaller: ToEntityMarshaller[Seq[GenePhenotypeAnnotation]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
    val header = "gene IRI\tgene label\tphenotype IRI\tphenotype label\tsource IRI"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboGenePhenotypeAnnotationsMarshaller = Marshaller.oneOf(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}