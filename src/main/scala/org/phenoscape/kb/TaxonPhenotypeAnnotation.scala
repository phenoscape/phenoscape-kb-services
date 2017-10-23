package org.phenoscape.kb

import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct
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

case class TaxonPhenotypeAnnotation(taxon: MinimalTerm, phenotype: MinimalTerm, study: MinimalTerm) extends JSONResultItem {

  def toJSON: JsObject = {
    (Map("taxon" -> taxon.toJSON,
      "phenotype" -> phenotype.toJSON,
      "study" -> study.toJSON)).toJson.asJsObject
  }

  override def toString(): String = {
    s"${taxon.iri}\t${taxon.label}\t${phenotype.iri}\t${phenotype.label}\t${study.iri}\t${study.label}"
  }

}

object TaxonPhenotypeAnnotation {

  def queryAnnotations(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Seq[TaxonPhenotypeAnnotation]] = for {
    query <- buildTaxonPhenotypeAnnotationsQuery(entity, quality, inTaxonOpt, limit, offset)
    annotations <- App.executeSPARQLQuery(query, fromQueryResult)
  } yield annotations

  def queryAnnotationsTotal(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Int] = for {
    query <- buildTaxonPhenotypeAnnotationsTotalQuery(entity, quality, inTaxonOpt)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def fromQueryResult(result: QuerySolution): TaxonPhenotypeAnnotation = TaxonPhenotypeAnnotation(
    MinimalTerm(IRI.create(result.getResource("taxon").getURI),
      result.getLiteral("taxon_label").getLexicalForm),
    MinimalTerm(IRI.create(result.getResource("phenotype").getURI),
      result.getLiteral("phenotype_label").getLexicalForm),
    MinimalTerm(IRI.create(result.getResource("matrix").getURI),
      result.getLiteral("matrix_label").getLexicalForm))

  private def buildBasicTaxonPhenotypeAnnotationsQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
    val phenotypeExpression = (entity, quality) match {
      case (Some(entityTerm), Some(qualityTerm)) => Some((has_part some qualityTerm) and (phenotype_of some entityTerm))
      case (Some(entityTerm), None)              => Some(phenotype_of some entityTerm)
      case (None, Some(qualityTerm))             => Some(has_part some qualityTerm)
      case (None, None)                          => None
    }
    val phenotypeTriple = phenotypeExpression.map(desc => t('phenotype, rdfsSubClassOf, desc.asOMN)).toList
    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf, _)).toList
    val query = select_distinct('taxon, 'taxon_label, 'phenotype, 'phenotype_label, 'matrix, 'matrix_label) where (
      bgp(
        App.BigdataAnalyticQuery ::
          t('state, describes_phenotype, 'phenotype) ::
          t('phenotype, rdfsLabel, 'phenotype_label) ::
          t('taxon, exhibits_state, 'state) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('matrix, rdfsLabel, 'matrix_label) ::
          t('taxon, rdfsLabel, 'taxon_label) ::
          phenotypeTriple ++
          taxonPatterns: _*))
    App.expandWithOwlet(query)
  }

  def buildTaxonPhenotypeAnnotationsQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI], limit: Int = 20, offset: Int = 0): Future[Query] = {
    for {
      rawQuery <- buildBasicTaxonPhenotypeAnnotationsQuery(entity, quality, inTaxonOpt)
    } yield {
      val query = rawQuery from KBMainGraph.toString from KBClosureGraph.toString
      query.setOffset(offset)
      if (limit > 0) query.setLimit(limit)
      query.addOrderBy('taxon_label)
      query.addOrderBy('taxon)
      query
    }
  }

  def buildTaxonPhenotypeAnnotationsTotalQuery(entity: Option[OWLClassExpression], quality: Option[OWLClassExpression], inTaxonOpt: Option[IRI]): Future[Query] = {
    for {
      rawQuery <- buildBasicTaxonPhenotypeAnnotationsQuery(entity, quality, inTaxonOpt)
    } yield {
      val query = select() from KBMainGraph.toString from KBClosureGraph.toString where (new ElementSubQuery(rawQuery))
      query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
      query
    }
  }

  val AnnotationTextMarshaller: ToEntityMarshaller[Seq[TaxonPhenotypeAnnotation]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
    val header = "taxon IRI\ttaxon label\tphenotype IRI\tphenotype label\tstudy IRI\tstudy label"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboTaxonPhenotypeAnnotationsMarshaller = Marshaller.oneOf(AnnotationTextMarshaller, JSONResultItemsMarshaller)

}