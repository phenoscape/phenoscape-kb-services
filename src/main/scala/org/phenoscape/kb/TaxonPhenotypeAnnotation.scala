package org.phenoscape.kb

import scala.concurrent.Future
import scala.language.postfixOps
import org.apache.jena.query.QuerySolution
import org.phenoscape.kb.Facets.Facet
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.kb.queries.TaxonAnnotations
import org.phenoscape.kb.KBVocab.KBMainGraph
import org.phenoscape.owl.Vocab._
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.sparql.SPARQLInterpolation._
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes
import org.phenoscape.kb.queries.QueryUtil.{PhenotypicQuality, QualitySpec}
import spray.json._
import spray.json.DefaultJsonProtocol._

final case class TaxonPhenotypeAnnotation(taxon: MinimalTerm, phenotype: MinimalTerm) extends JSONResultItem {

  def toJSON: JsObject = {
    (Map(
      "taxon" -> taxon.toJSON,
      "phenotype" -> phenotype.toJSON)).toJson.asJsObject
  }

  override def toString(): String = {
    s"${taxon.iri}\t${taxon.label}\t${phenotype.iri}\t${phenotype.label}"
  }

}

object TaxonPhenotypeAnnotation {

  def queryAnnotations(entity: Option[IRI], quality: QualitySpec, inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, limit: Int = 20, offset: Int = 0): Future[Seq[TaxonPhenotypeAnnotation]] = for {
    query <- TaxonAnnotations.buildQuery(entity, quality, inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, false, limit, offset)
    annotations <- App.executeSPARQLQueryString(query, fromQueryResult)
  } yield annotations

  def queryAnnotationsTotal(entity: Option[IRI], quality: QualitySpec, inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Int] = for {
    query <- TaxonAnnotations.buildQuery(entity, quality, inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def fromQueryResult(result: QuerySolution): TaxonPhenotypeAnnotation = TaxonPhenotypeAnnotation(
    MinimalTerm(
      IRI.create(result.getResource("taxon").getURI),
      result.getLiteral("taxon_label").getLexicalForm),
    MinimalTerm(
      IRI.create(result.getResource("phenotype").getURI),
      result.getLiteral("phenotype_label").getLexicalForm))

  def facetTaxonAnnotationsByEntity(focalEntity: Option[IRI], quality: QualitySpec, inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryAnnotationsTotal(Some(iri), quality, inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.queryAnatomySubClasses(iri, KBVocab.Uberon, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine, false)
  }

  def facetTaxonAnnotationsByQuality(focalQuality: Option[IRI], entity: Option[IRI], inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryAnnotationsTotal(entity, PhenotypicQuality(Some(iri)), inTaxonOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine, false)
  }

  def facetTaxonAnnotationsByTaxon(focalTaxon: Option[IRI], entity: Option[IRI], quality: QualitySpec, publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryAnnotationsTotal(entity, quality, Some(iri), publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.VTO)).map(_.toSet)
    Facets.facet(focalTaxon.getOrElse(KBVocab.taxonRoot), query, refine, true)
  }

  def annotationSources(taxon: IRI, phenotype: IRI): Future[Seq[AnnotationSource]] = {
    val query = sparql"""
      SELECT DISTINCT ?pub ?pub_label ?char_num ?char_text ?state_text
      FROM $KBMainGraph
      WHERE {
        $taxon $exhibits_state ?state .
        ?state $describes_phenotype $phenotype .
        ?pub $has_character ?character .
        ?character $may_have_state_value ?state .
        ?character $list_index ?char_num .
        ?character $rdfsLabel ?char_text .
        ?state $rdfsLabel ?state_text .
        ?pub $rdfsLabel ?pub_label .
      }
      """
    App.executeSPARQLQueryString(query.text, res => {
      AnnotationSource(MinimalTerm(IRI.create(res.getResource("pub").getURI), res.getLiteral("pub_label").getLexicalForm), res.getLiteral("char_num").getInt, res.getLiteral("char_text").getLexicalForm, res.getLiteral("state_text").getLexicalForm)
    })
  }

  private def facetResultToMap(facets: List[(MinimalTerm, Int)]) = Map("facets" -> facets.map { case (term, count) => Map("term" -> term, "count" -> count) })

  val AnnotationTextMarshaller: ToEntityMarshaller[Seq[TaxonPhenotypeAnnotation]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
    val header = "taxon IRI\ttaxon label\tphenotype IRI\tphenotype label"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboTaxonPhenotypeAnnotationsMarshaller = Marshaller.oneOf(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}

final case class AnnotationSource(publication: MinimalTerm, characterNum: Int, character: String, state: String) extends JSONResultItem {

  def toJSON: JsObject = {
    (Map(
      "publication" -> publication.toJSON,
      "character_num" -> characterNum.toJson,
      "character_text" -> character.toJson,
      "state_text" -> state.toJson)).toJson.asJsObject
  }

}
