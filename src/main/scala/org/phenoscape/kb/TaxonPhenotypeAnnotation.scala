package org.phenoscape.kb

import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.query.QuerySolution
import org.phenoscape.kb.Facets.Facet
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.kb.queries.TaxonAnnotations
import org.semanticweb.owlapi.model.IRI

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes
import spray.json._
import spray.json.DefaultJsonProtocol._

case class TaxonPhenotypeAnnotation(taxon: MinimalTerm, phenotype: MinimalTerm, study: MinimalTerm) extends JSONResultItem {

  def toJSON: JsObject = {
    (Map(
      "taxon" -> taxon.toJSON,
      "phenotype" -> phenotype.toJSON,
      "study" -> study.toJSON)).toJson.asJsObject
  }

  override def toString(): String = {
    s"${taxon.iri}\t${taxon.label}\t${phenotype.iri}\t${phenotype.label}\t${study.iri}\t${study.label}"
  }

}

object TaxonPhenotypeAnnotation {

  def queryAnnotations(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, limit: Int = 20, offset: Int = 0): Future[Seq[TaxonPhenotypeAnnotation]] = for {
    query <- TaxonAnnotations.buildQuery(entity, quality, inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, false, limit, offset)
    annotations <- App.executeSPARQLQueryString(query, fromQueryResult)
  } yield annotations

  def queryAnnotationsTotal(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Int] = for {
    query <- TaxonAnnotations.buildQuery(entity, quality, inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def fromQueryResult(result: QuerySolution): TaxonPhenotypeAnnotation = TaxonPhenotypeAnnotation(
    MinimalTerm(
      IRI.create(result.getResource("taxon").getURI),
      result.getLiteral("taxon_label").getLexicalForm),
    MinimalTerm(
      IRI.create(result.getResource("phenotype").getURI),
      result.getLiteral("phenotype_label").getLexicalForm),
    MinimalTerm(
      IRI.create(result.getResource("matrix").getURI),
      result.getLiteral("matrix_label").getLexicalForm))

  def facetTaxonAnnotationsByEntity(focalEntity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryAnnotationsTotal(Some(iri), quality, inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.Uberon)).map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine)
  }

  def facetTaxonAnnotationsByQuality(focalQuality: Option[IRI], entity: Option[IRI], inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryAnnotationsTotal(entity, Some(iri), inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine)
  }

  def facetTaxonAnnotationsByTaxon(focalTaxon: Option[IRI], entity: Option[IRI], quality: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryAnnotationsTotal(entity, quality, Some(iri), publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.VTO)).map(_.toSet)
    Facets.facet(focalTaxon.getOrElse(KBVocab.taxonRoot), query, refine)
  }

  private def facetResultToMap(facets: List[(MinimalTerm, Int)]) = Map("facets" -> facets.map { case (term, count) => Map("term" -> term, "count" -> count) })

  val AnnotationTextMarshaller: ToEntityMarshaller[Seq[TaxonPhenotypeAnnotation]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
    val header = "taxon IRI\ttaxon label\tphenotype IRI\tphenotype label\tstudy IRI\tstudy label"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboTaxonPhenotypeAnnotationsMarshaller = Marshaller.oneOf(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}