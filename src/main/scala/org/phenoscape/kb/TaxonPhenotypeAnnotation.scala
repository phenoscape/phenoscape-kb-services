package org.phenoscape.kb

import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.query.QuerySolution
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

  def queryAnnotations(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, limit: Int = 20, offset: Int = 0): Future[Seq[TaxonPhenotypeAnnotation]] = for {
    query <- TaxonAnnotations.buildQuery(entity, quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, false, limit, offset)
    _ = println(query)
    annotations <- App.executeSPARQLQueryString(query, fromQueryResult)
  } yield annotations

  def queryAnnotationsTotal(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Int] = for {
    query <- TaxonAnnotations.buildQuery(entity, quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
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

  //  private def buildBasicTaxonPhenotypeAnnotationsQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
  //    val phenotypeExpression = (entity, quality) match {
  //      case (Some(entityTerm), Some(qualityTerm)) => Some((has_part some qualityTerm) and (phenotype_of some entityTerm))
  //      case (Some(entityTerm), None)              => Some(phenotype_of some entityTerm)
  //      case (None, Some(qualityTerm))             => Some(has_part some qualityTerm)
  //      case (None, None)                          => None
  //    }
  //    val phenotypeTriple = phenotypeExpression.map(desc => t('phenotype, rdfsSubClassOf, desc.asOMN)).toList
  //    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf, _)).toList
  //    val query = select_distinct('taxon, 'taxon_label, 'phenotype, 'phenotype_label, 'matrix, 'matrix_label) where (
  //      bgp(
  //        App.BigdataAnalyticQuery ::
  //          t('state, describes_phenotype, 'phenotype) ::
  //          t('phenotype, rdfsLabel, 'phenotype_label) ::
  //          t('taxon, exhibits_state, 'state) ::
  //          t('matrix, has_character / may_have_state_value, 'state) ::
  //          t('matrix, rdfsLabel, 'matrix_label) ::
  //          t('taxon, rdfsLabel, 'taxon_label) ::
  //          phenotypeTriple ++
  //          taxonPatterns: _*))
  //    App.expandWithOwlet(query)
  //  }
  //
  //  def buildTaxonPhenotypeAnnotationsQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Query] = {
  //    for {
  //      rawQuery <- buildBasicTaxonPhenotypeAnnotationsQuery(entity, quality, inTaxonOpt)
  //    } yield {
  //      val query = rawQuery from KBMainGraph.toString from KBClosureGraph.toString
  //      query.setOffset(offset)
  //      if (limit > 0) query.setLimit(limit)
  //      query.addOrderBy('taxon_label)
  //      query.addOrderBy('taxon)
  //      query
  //    }
  //  }
  //
  //  def buildTaxonPhenotypeAnnotationsTotalQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
  //    for {
  //      rawQuery <- buildBasicTaxonPhenotypeAnnotationsQuery(entity, quality, inTaxonOpt)
  //    } yield {
  //      val query = select() from KBMainGraph.toString from KBClosureGraph.toString where (new ElementSubQuery(rawQuery))
  //      query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
  //      query
  //    }
  //  }

  val AnnotationTextMarshaller: ToEntityMarshaller[Seq[TaxonPhenotypeAnnotation]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
    val header = "taxon IRI\ttaxon label\tphenotype IRI\tphenotype label\tstudy IRI\tstudy label"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboTaxonPhenotypeAnnotationsMarshaller = Marshaller.oneOf(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}