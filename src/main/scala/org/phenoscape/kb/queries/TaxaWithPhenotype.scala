package org.phenoscape.kb.queries

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

import org.apache.jena.vocabulary.RDFS
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClassExpression
import org.phenoscape.kb.App
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolation.QueryText
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import scalaz._
import Scalaz._
import org.phenoscape.kb.util.BlazegraphNamedSubquery
import org.phenoscape.owl.NamedRestrictionGenerator
import org.semanticweb.owlapi.model.OWLObjectProperty
import org.phenoscape.kb.AnatomicalEntity
import org.phenoscape.kb.HomologyAnnotation

object TaxaWithPhenotype {

  private val PhenotypeOfSome = NamedRestrictionGenerator.getClassRelationIRI(phenotype_of.getIRI)
  private val PartOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)
  private val HasPartSome = NamedRestrictionGenerator.getClassRelationIRI(has_part.getIRI)

  def buildQuery(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, countOnly: Boolean, limit: Int, offset: Int): Future[String] = {
    for {
      (whereClause, subqueries) <- constructWhereClause(entity, quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    } yield {
      val unifiedQueries = BlazegraphNamedSubquery.unifyQueries(subqueries)
      val namedQueriesBlock = if (unifiedQueries.nonEmpty) unifiedQueries.map(_.namedQuery).reduce(_ |+| _) else sparql""
      val query = if (countOnly) sparql"""
      SELECT (COUNT(*) AS ?count)
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      WHERE {
        SELECT DISTINCT ?taxon ?taxon_label
        $whereClause
      }
      """
      else sparql"""
      SELECT DISTINCT ?taxon ?taxon_label
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      $whereClause
      ORDER BY ?taxon_label ?taxon
      LIMIT $limit OFFSET $offset
      """
      BlazegraphNamedSubquery.updateReferencesFor(unifiedQueries, query.text)
    }
  }

  //FIXME this should query for homology annotations for all subclasses of entity
  private def constructWhereClause(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[(QueryText, Set[BlazegraphNamedSubquery])] = {
    val validHomologyRelation = (if (includeHistoricalHomologs) Set(homologous_to.getIRI) else Set.empty) ++ (if (includeSerialHomologs) Set(serially_homologous_to.getIRI) else Set.empty)
    val homologyQueryPartsFut: ListT[Future, (List[QueryText], Set[BlazegraphNamedSubquery])] = for {
      entityTerm <- entity.toList |> Future.successful |> ListT.apply
      if includeHistoricalHomologs || includeSerialHomologs
      annotations <- AnatomicalEntity.homologyAnnotations(entityTerm).map(List(_)) |> ListT.apply
      uniquedPositiveAnnotations = annotations.filterNot(_.negated).map(ann => (ann.`object`, ann.objectTaxon, ann.relation)).toSet
      (otherEntity, otherTaxon, relation) <- uniquedPositiveAnnotations.toList |> Future.successful |> ListT.apply
      if validHomologyRelation(relation)
    } yield {
      var homComponents = List.empty[QueryText]
      var homSubqueries = Set.empty[BlazegraphNamedSubquery]
      val homSubquery = phenotypeSubQueryFor(Option(otherEntity), quality, false)
      val basicHom = coreTaxonToPhenotype(inTaxonOpt.toSet + otherTaxon, homSubquery)
      homComponents = basicHom :: homComponents
      homSubquery.foreach(q => homSubqueries += q)
      if (includeParts) {
        val homPartsSubquery = phenotypeSubQueryFor(Option(otherEntity), quality, true)
        val homParts = coreTaxonToPhenotype(inTaxonOpt.toSet + otherTaxon, homPartsSubquery)
        homComponents = homParts :: homComponents
        homPartsSubquery.foreach(q => homSubqueries += q)
      }
      (homComponents, homSubqueries)
    }
    for {
      homologyQueryParts <- homologyQueryPartsFut.run
    } yield {
      val (homologyWhereBlocks, homologySubqueries) = homologyQueryParts.unzip

      var components = homologyWhereBlocks.flatten
      var subqueries = homologySubqueries.toSet.flatten
      val basicSubquery = phenotypeSubQueryFor(entity, quality, false)
      val basic = coreTaxonToPhenotype(inTaxonOpt.toSet, basicSubquery)
      components = basic :: components
      basicSubquery.foreach(q => subqueries += q)
      if (includeParts) {
        val partsSubquery = phenotypeSubQueryFor(entity, quality, true)
        val parts = coreTaxonToPhenotype(inTaxonOpt.toSet, partsSubquery)
        components = parts :: components
        partsSubquery.foreach(q => subqueries += q)
      }
      val blocks = (components match {
        case Nil          => List(sparql"")
        case head :: Nil  => components
        case head :: tail => head :: tail.map(sparql" UNION " |+| _)
      }).reduce(_ |+| _)
      sparql"""
      WHERE {
        $blocks
      }
      """ -> subqueries
    }
  }

  def phenotypeSubQueryFor(entity: Option[IRI], quality: Option[IRI], parts: Boolean): Option[BlazegraphNamedSubquery] = if (entity.nonEmpty || quality.nonEmpty) {
    val entityPattern = entity.map { e =>
      if (parts) sparql"?p $rdfsSubClassOf/$PhenotypeOfSome/$rdfsSubClassOf/$PartOfSome $e . "
      else sparql"?p $rdfsSubClassOf/$PhenotypeOfSome $e . "
    }.getOrElse(sparql"")
    val qualityPattern = quality.map(q => sparql"?p $rdfsSubClassOf/$HasPartSome $q . ").getOrElse(sparql"")
    Some(BlazegraphNamedSubquery(sparql"""
        SELECT ?phenotype WHERE {
          $entityPattern
          $qualityPattern
          GRAPH $KBMainGraph {
            ?phenotype $rdfsSubClassOf ?p .
          }
        }
      """))
  } else None

  private def coreTaxonToPhenotype(inTaxa: Set[IRI], phenotypeQuery: Option[BlazegraphNamedSubquery]): QueryText = {
    val taxonConstraints = (for { taxon <- inTaxa }
      yield sparql"?taxon $rdfsSubClassOf $taxon . ").fold(sparql"")(_ |+| _)
    val subQueryRef = phenotypeQuery.map(q => sparql"$q").getOrElse(sparql"")
    sparql"""
      {
      ?taxon $RDFSLabel ?taxon_label .
      ?taxon $exhibits_state ?state .
      ?state $describes_phenotype ?phenotype .
      $taxonConstraints
      $subQueryRef
      }
    """
  }

}