package org.phenoscape.kb

import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.language.postfixOps
import spray.http._
import spray.httpx._
import spray.httpx.marshalling._
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.scowl._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery
import org.semanticweb.owlapi.model.OWLClassExpression
import com.hp.hpl.jena.query.Query
import org.semanticweb.owlapi.model.IRI
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountDistinct
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.E_NotOneOf
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import scala.io.Source

import akka.util.Timeout
import scala.concurrent.duration._

case class GeneExpressionAnnotation(gene: MinimalTerm, location: MinimalTerm, source: Option[IRI]) extends JSONResultItem {

  def toJSON: JsObject = {
    (Map("gene" -> gene.toJSON,
      "location" -> location.toJSON,
      "source" -> source.map(_.toString).getOrElse("").toJson)).toJson.asJsObject
  }

  override def toString(): String = {
    s"${gene.iri}\t${gene.label}\t${location.iri}\t${location.label}\t${source}"
  }

}

object GeneExpressionAnnotation {

  implicit val timeout = Timeout(10 minutes)

  def queryAnnotations(entity: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Seq[GeneExpressionAnnotation]] = for {
    query <- buildGeneExpressionAnnotationsQuery(entity, inTaxonOpt, limit, offset)
    annotations <- App.executeSPARQLQuery(query, fromQueryResult)
  } yield annotations

  def queryAnnotationsTotal(entity: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Int] = for {
    query <- buildGeneExpressionAnnotationsTotalQuery(entity, inTaxonOpt)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def fromQueryResult(result: QuerySolution): GeneExpressionAnnotation = GeneExpressionAnnotation(
    MinimalTerm(IRI.create(result.getResource("gene").getURI),
      result.getLiteral("gene_label").getLexicalForm),
    MinimalTerm(IRI.create(result.getResource("location").getURI),
      result.getLiteral("location_label").getLexicalForm),
    Option(result.getResource("source")).map(v => IRI.create(v.getURI)))

  private def buildBasicGeneExpressionAnnotationsQuery(entity: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
    val locationExpression = entity.map(e => Class(ANATOMICAL_ENTITY) and (part_of some e))
    val phenotypeTriple = locationExpression.map(desc => t('location, rdfsSubClassOf, desc.asOMN)).toList
    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf*, _)).toList
    val query = select_distinct('gene, 'gene_label, 'location, 'location_label, 'source) where (
      bgp(
        App.BigdataAnalyticQuery ::
          //t('annotation, rdfType, GeneExpression) ::
          t('annotation, associated_with_gene, 'gene) ::
          t('gene, rdfsLabel, 'gene_label) ::
          t('annotation, occurs_in / rdfType, 'location) ::
          t('location, rdfsLabel, 'location_label) ::
          //t('annotation, associated_with_taxon, 'taxon) ::
          phenotypeTriple ++
          taxonPatterns: _*),
        optional(bgp(t('annotation, dcSource, 'source))),
        new ElementFilter(new E_NotOneOf(new ExprVar('location), new ExprList(List(new NodeValueNode(owlNamedIndividual))))))
    App.expandWithOwlet(query)
  }

  def buildGeneExpressionAnnotationsQuery(entity: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Query] = {
    for {
      rawQuery <- buildBasicGeneExpressionAnnotationsQuery(entity, inTaxonOpt)
    } yield {
      val query = rawQuery from KBMainGraph
      query.setOffset(offset)
      if (limit > 0) query.setLimit(limit)
      query.addOrderBy('gene_label)
      query.addOrderBy('gene)
      query.addOrderBy('location_label)
      query.addOrderBy('location)
      query
    }
  }

  def buildGeneExpressionAnnotationsTotalQuery(entity: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
    for {
      rawQuery <- buildBasicGeneExpressionAnnotationsQuery(entity, inTaxonOpt)
    } yield {
      val query = select() from KBMainGraph where (new ElementSubQuery(rawQuery))
      query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
      query
    }
  }

  val AnnotationTextMarshaller = Marshaller.delegate[Seq[GeneExpressionAnnotation], String](MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`) { annotations =>
    val header = "gene IRI\tgene label\tlocation IRI\tlocation label\tsource IRI"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboGeneExpressionAnnotationsMarshaller = ToResponseMarshaller.oneOf(MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`, MediaTypes.`application/json`)(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}