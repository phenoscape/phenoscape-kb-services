package org.phenoscape.kb.queries

import org.phenoscape.kb.AnatomicalEntity
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.queries.QueryUtil.QualitySpec
import org.phenoscape.kb.util.BlazegraphNamedSubquery
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.owl.Vocab._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}
import org.semanticweb.owlapi.model.IRI
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.Future
import scala.language.postfixOps

object DirectPhenotypesForTaxon {

  def buildQuery(taxon: IRI, entity: Option[IRI], quality: QualitySpec, phenotypeOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, countOnly: Boolean, limit: Int, offset: Int): Future[String] = {
    for {
      (whereClause, subqueries) <- constructWhereClause(taxon, entity, quality, phenotypeOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
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
        SELECT DISTINCT ?state ?description ?matrix ?matrix_label ?phenotype
        $whereClause
      }
      """
      else
        sparql"""
      SELECT DISTINCT ?state ?description ?matrix ?matrix_label ?phenotype ?character ?character_label
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      $whereClause
      ORDER BY LCASE(?description) ?phenotype
      $paging
      """
      BlazegraphNamedSubquery.updateReferencesFor(unifiedQueries, query.text)
    }
  }

  //TODO extract common parts with TaxaWithPhenotype
  private def constructWhereClause(taxon: IRI, entity: Option[IRI], quality: QualitySpec, phenotypeOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[(QueryText, Set[BlazegraphNamedSubquery])] = {
    val validHomologyRelation = (if (includeHistoricalHomologs) Set(homologous_to.getIRI) else Set.empty[IRI]) ++ (if (includeSerialHomologs) Set(serially_homologous_to.getIRI) else Set.empty[IRI])
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
      val homSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(Option(otherEntity), quality, phenotypeOpt, false)
      val basicHom = coreTaxonToPhenotype(taxon, Set(otherTaxon), homSubquery)
      homComponents = basicHom :: homComponents
      homSubquery.foreach(q => homSubqueries += q)
      if (includeParts) {
        val homPartsSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(Option(otherEntity), quality, phenotypeOpt, true)
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
      val basicSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(entity, quality, phenotypeOpt, false)
      val basic = coreTaxonToPhenotype(taxon, Set.empty, basicSubquery)
      components = basic :: components
      basicSubquery.foreach(q => subqueries += q)
      if (includeParts) {
        val partsSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(entity, quality, phenotypeOpt, true)
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
    val taxonConstraints = (for {inTaxon <- inTaxa}
      yield sparql"FILTER EXISTS { $taxon $rdfsSubClassOf $inTaxon . }").fold(sparql"")(_ |+| _)
    val subQueryRefs = QueryText(phenotypeQueries.map(q => sparql"$q").map(_.text).mkString("\n"))
    sparql"""
      {
      $taxon $exhibits_state ?state .
      ?state $describes_phenotype ?phenotype .
      ?state $dcDescription ?description .
      ?matrix $has_character ?character .
      ?character $may_have_state_value ?state .
      ?character $rdfsLabel ?character_label .
      ?matrix $rdfsLabel ?matrix_label .
      $taxonConstraints
      $subQueryRefs
      }
    """
  }

}