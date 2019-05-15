package org.phenoscape.kb.queries

import org.phenoscape.kb.AnatomicalEntity
import org.phenoscape.kb.KBVocab.{rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.util.BlazegraphNamedSubquery
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab._
import org.phenoscape.scowl._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}
import org.semanticweb.owlapi.model.IRI
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.Future
import scala.language.postfixOps

object GeneAffectingPhenotype {

  private val PhenotypeOfSome = NamedRestrictionGenerator.getClassRelationIRI(phenotype_of.getIRI)
  private val PartOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)
  private val HasPartSome = NamedRestrictionGenerator.getClassRelationIRI(has_part.getIRI)

  def buildQuery(entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, countOnly: Boolean, limit: Int, offset: Int): Future[String] = {
    for {
      (whereClause, subqueries) <- constructWhereClause(entity, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    } yield {
      val unifiedQueries = BlazegraphNamedSubquery.unifyQueries(subqueries)
      val namedQueriesBlock = if (unifiedQueries.nonEmpty) unifiedQueries.map(_.namedQuery).reduce(_ |+| _) else sparql""
      val paging = if (limit > 0) sparql"LIMIT $limit OFFSET $offset" else sparql""
      val query = if (countOnly)
        sparql"""
      SELECT (COUNT(*) AS ?count)
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      WHERE {
        SELECT DISTINCT ?gene ?gene_label
        $whereClause
      }
      """
      else
        sparql"""
      SELECT DISTINCT ?gene ?gene_label
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      $whereClause
      ORDER BY LCASE(?gene_label) ?gene
      $paging
      """
      BlazegraphNamedSubquery.updateReferencesFor(unifiedQueries, query.text)
    }
  }

  private def constructWhereClause(entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[(QueryText, Set[BlazegraphNamedSubquery])] = {
    val validHomologyRelation = (if (includeHistoricalHomologs) Set(homologous_to.getIRI) else Set.empty) ++ (if (includeSerialHomologs) Set(serially_homologous_to.getIRI) else Set.empty)
    val homologyQueryPartsFut: ListT[Future, (List[QueryText], Set[BlazegraphNamedSubquery])] = for {
      entityTerm <- entity.toList |> Future.successful |> ListT.apply
      if includeHistoricalHomologs || includeSerialHomologs
      annotations <- AnatomicalEntity.homologyAnnotations(entityTerm, true).map(List(_)) |> ListT.apply
      uniquedPositiveAnnotations = annotations.filterNot(_.negated).map(ann => (ann.`object`, ann.objectTaxon, ann.relation)).toSet
      (otherEntity, otherTaxon, relation) <- uniquedPositiveAnnotations.toList |> Future.successful |> ListT.apply
      if validHomologyRelation(relation)
    } yield {
      var homComponents = List.empty[QueryText]
      var homSubqueries = Set.empty[BlazegraphNamedSubquery]
      val homSubquery = phenotypeSubQueryFor(Option(otherEntity), quality, false)
      //FIXME add taxon links to genes and use taxon restriction in homology queries for gene data
      val basicHom = coreGeneToPhenotype(homSubquery)
      homComponents = basicHom :: homComponents
      homSubquery.foreach(q => homSubqueries += q)
      if (includeParts) {
        val homPartsSubquery = phenotypeSubQueryFor(Option(otherEntity), quality, true)
        val homParts = coreGeneToPhenotype(homPartsSubquery)
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
      val basic = coreGeneToPhenotype(basicSubquery)
      components = basic :: components
      basicSubquery.foreach(q => subqueries += q)
      if (includeParts) {
        val partsSubquery = phenotypeSubQueryFor(entity, quality, true)
        val parts = coreGeneToPhenotype(partsSubquery)
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

  private def coreGeneToPhenotype(phenotypeQuery: Option[BlazegraphNamedSubquery]): QueryText = {
    val subQueryRef = phenotypeQuery.map(q => sparql"$q").getOrElse(sparql"")
    sparql"""
      {
      ?gene $RDFSLabel ?gene_label .
      ?gene $rdfType $Gene .
      ?gene $has_phenotypic_profile ?phenotype .
      $subQueryRef
      }
    """
  }

  // named subquery is used like for taxon query, but may not really be necessary
  def phenotypeSubQueryFor(entity: Option[IRI], quality: Option[IRI], parts: Boolean): Option[BlazegraphNamedSubquery] = if (entity.nonEmpty || quality.nonEmpty) {
    val entityPattern = entity.map { e =>
      if (parts) sparql"?phenotype $rdfType/$PhenotypeOfSome/$rdfsSubClassOf/$PartOfSome $e . "
      else sparql"?phenotype $rdfType/$PhenotypeOfSome $e . "
    }.getOrElse(sparql"")
    val qualityPattern = quality.map(q => sparql"?phenotype $rdfType/$HasPartSome $q . ").getOrElse(sparql"")
    Some(BlazegraphNamedSubquery(
      sparql"""
        SELECT DISTINCT ?phenotype WHERE {
          $entityPattern
          $qualityPattern
        }
      """))
  } else None

}