package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import akka.util.Timeout
import org.apache.jena.query.{Query, QuerySolution}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.expr.{E_NotOneOf, Expr, ExprList, ExprVar}
import org.apache.jena.sparql.syntax.{ElementFilter, ElementSubQuery}
import org.phenoscape.kb.JSONResultItem.JSONResultItemsMarshaller
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.{IRI, OWLClassExpression}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

case class GeneExpressionAnnotation(gene: MinimalTerm, location: MinimalTerm, source: Option[IRI])
    extends JSONResultItem {

  def toJSON: JsObject =
    Map("gene" -> gene.toJSON,
        "location" -> location.toJSON,
        "source" -> source.map(_.toString).getOrElse("").toJson).toJson.asJsObject

  override def toString: String =
    s"${gene.iri}\t${gene.label}\t${location.iri}\t${location.label}\t$source"

}

object GeneExpressionAnnotation {

  implicit val timeout: Timeout = Timeout(10 minutes)

  def queryAnnotations(entity: Option[OWLClassExpression],
                       inTaxonOpt: Option[IRI],
                       limit: Int = 20,
                       offset: Int = 0): Future[Seq[GeneExpressionAnnotation]] =
    for {
      query <- buildGeneExpressionAnnotationsQuery(entity, inTaxonOpt, limit, offset)
      annotations <- App.executeSPARQLQuery(query, fromQueryResult)
    } yield annotations

  def queryAnnotationsTotal(entity: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Int] =
    for {
      query <- buildGeneExpressionAnnotationsTotalQuery(entity, inTaxonOpt)
      result <- App.executeSPARQLQuery(query)
    } yield ResultCount.count(result)

  def fromQueryResult(result: QuerySolution): GeneExpressionAnnotation =
    GeneExpressionAnnotation(
      MinimalTerm(IRI.create(result.getResource("gene").getURI), Some(result.getLiteral("gene_label").getLexicalForm)),
      MinimalTerm(IRI.create(result.getResource("location").getURI),
                  Option(result.getLiteral("location_label")).map(v => v.getLexicalForm)),
      Option(result.getResource("source")).map(v => IRI.create(v.getURI))
    )

  private def buildBasicGeneExpressionAnnotationsQuery(entity: Option[OWLClassExpression],
                                                       inTaxonOpt: Option[IRI]): Future[Query] = {
    val locationExpression = entity.map(e => Class(ANATOMICAL_ENTITY) and (part_of some e))
    val phenotypeTriple = locationExpression.map(desc => t('location, rdfsSubClassOf, desc.asOMN)).toList
    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf *, _)).toList
    val query = select_distinct('gene, 'gene_label, 'location, 'location_label, 'source) where (bgp(
      App.BigdataAnalyticQuery ::
        //t('annotation, rdfType, GeneExpression) ::
        t('annotation, associated_with_gene, 'gene) ::
        t('gene, rdfsLabel, 'gene_label) ::
        t('annotation, occurs_in / rdfType, 'location) ::
        t('annotation, associated_with_taxon, 'taxon) ::
        phenotypeTriple ++
        taxonPatterns: _*
    ),
    optional(bgp(t('location, rdfsLabel, 'location_label))),
    optional(bgp(t('annotation, dcSource, 'source))),
    new ElementFilter(
      new E_NotOneOf(new ExprVar('location), new ExprList(List[Expr](new NodeValueNode(owlNamedIndividual)).asJava))))
    App.expandWithOwlet(query)
  }

  def buildGeneExpressionAnnotationsQuery(entity: Option[OWLClassExpression],
                                          inTaxonOpt: Option[IRI],
                                          limit: Int = 20,
                                          offset: Int = 0): Future[Query] =
    for {
      rawQuery <- buildBasicGeneExpressionAnnotationsQuery(entity, inTaxonOpt)
    } yield {
      val query = rawQuery from KBMainGraph.toString
      query.setOffset(offset)
      if (limit > 0) query.setLimit(limit)
      query.addOrderBy('gene_label)
      query.addOrderBy('gene)
      query.addOrderBy('location_label)
      query.addOrderBy('location)
      query
    }

  def buildGeneExpressionAnnotationsTotalQuery(entity: Option[OWLClassExpression],
                                               inTaxonOpt: Option[IRI]): Future[Query] =
    for {
      rawQuery <- buildBasicGeneExpressionAnnotationsQuery(entity, inTaxonOpt)
    } yield {
      val query = select() from KBMainGraph.toString where new ElementSubQuery(rawQuery)
      query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
      query
    }

  implicit val AnnotationTextMarshaller: ToEntityMarshaller[Seq[GeneExpressionAnnotation]] =
    Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
      val header = "gene IRI\tgene label\tlocation IRI\tlocation label\tsource IRI"
      s"$header\n${annotations.map(_.toString).mkString("\n")}"
    }

  implicit val ComboGeneExpressionAnnotationsMarshaller: ToEntityMarshaller[Seq[GeneExpressionAnnotation]] =
    Marshaller.oneOf(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}
