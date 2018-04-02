package org.phenoscape.kb.queries

import scala.concurrent.Future
import scala.language.postfixOps

import org.phenoscape.kb.AnatomicalEntity
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.util.BlazegraphNamedSubquery
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.owl.Vocab._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolation.QueryText
import org.semanticweb.owlapi.model.IRI

import scalaz._
import scalaz.Scalaz._

object DirectPhenotypesForTaxon {

  def buildQuery(taxon: IRI, entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, countOnly: Boolean, limit: Int, offset: Int): Future[String] = {
    for {
      (whereClause, subqueries) <- constructWhereClause(taxon, entity, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    } yield {
      val unifiedQueries = BlazegraphNamedSubquery.unifyQueries(subqueries)
      val namedQueriesBlock = if (unifiedQueries.nonEmpty) unifiedQueries.map(_.namedQuery).reduce(_ |+| _) else sparql""
      val query = if (countOnly) sparql"""
      SELECT (COUNT(*) AS ?count)
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      WHERE {
        SELECT DISTINCT ?state ?description ?matrix ?matrix_label ?phenotype
        $whereClause
      }
      """
      else sparql"""
      SELECT DISTINCT ?state ?description ?matrix ?matrix_label ?phenotype
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      $whereClause
      ORDER BY LCASE(?description) ?phenotype
      LIMIT $limit OFFSET $offset
      """
      BlazegraphNamedSubquery.updateReferencesFor(unifiedQueries, query.text)
    }
  }

  //TODO extract common parts with TaxaWithPhenotype
  private def constructWhereClause(taxon: IRI, entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[(QueryText, Set[BlazegraphNamedSubquery])] = {
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
      val homSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(Option(otherEntity), quality, false)
      val basicHom = coreTaxonToPhenotype(taxon, Set(otherTaxon), homSubquery)
      homComponents = basicHom :: homComponents
      homSubquery.foreach(q => homSubqueries += q)
      if (includeParts) {
        val homPartsSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(Option(otherEntity), quality, true)
        val homParts = coreTaxonToPhenotype(taxon, Set(otherTaxon), homPartsSubquery)
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
      val basicSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(entity, quality, false)
      val basic = coreTaxonToPhenotype(taxon, Set.empty, basicSubquery)
      components = basic :: components
      basicSubquery.foreach(q => subqueries += q)
      if (includeParts) {
        val partsSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(entity, quality, true)
        val parts = coreTaxonToPhenotype(taxon, Set.empty, partsSubquery)
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

  private def coreTaxonToPhenotype(taxon: IRI, inTaxa: Set[IRI], phenotypeQueries: Set[BlazegraphNamedSubquery]): QueryText = {
    // triple pattern without variables must go inside filter
    val taxonConstraints = (for { inTaxon <- inTaxa }
      yield sparql"FILTER EXISTS { $taxon $rdfsSubClassOf $inTaxon . }").fold(sparql"")(_ |+| _)
    val subQueryRefs = QueryText(phenotypeQueries.map(q => sparql"$q").map(_.text).mkString("\n"))
    sparql"""
      {
      $taxon $exhibits_state ?state .
      ?state $describes_phenotype ?phenotype .
      ?state $dcDescription ?description .
      ?matrix $has_character/$may_have_state_value ?state .
      ?matrix $rdfsLabel ?matrix_label .
      $taxonConstraints
      $subQueryRefs
      }
    """
  }

}