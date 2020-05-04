package org.phenoscape.kb.queries

import org.phenoscape.kb.AnatomicalEntity
import org.phenoscape.kb.KBVocab.{rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.queries.QueryUtil.QualitySpec
import org.phenoscape.kb.util.BlazegraphNamedSubquery
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.owl.Vocab._
import org.phenoscape.scowl._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}
import org.semanticweb.owlapi.model.IRI
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.Future
import scala.language.postfixOps

object TaxonPhenotypes {

  def buildQuery(
      entity: Option[IRI],
      quality: QualitySpec,
      inTaxon: Option[IRI],
      phenotypeOpt: Option[IRI],
      publicationOpt: Option[IRI],
      includeParts: Boolean,
      includeHistoricalHomologs: Boolean,
      includeSerialHomologs: Boolean,
      countOnly: Boolean,
      limit: Int,
      offset: Int
  ): Future[String] = {
    for {
      (whereClause, subqueries) <- constructWhereClause(
        entity,
        quality,
        inTaxon,
        phenotypeOpt,
        publicationOpt,
        includeParts,
        includeHistoricalHomologs,
        includeSerialHomologs
      )
    } yield {
      val unifiedQueries = BlazegraphNamedSubquery.unifyQueries(subqueries)
      val namedQueriesBlock =
        if (unifiedQueries.nonEmpty)
          unifiedQueries.map(_.namedQuery).reduce(_ |+| _)
        else sparql""
      val paging =
        if (limit > 0) sparql"LIMIT $limit OFFSET $offset" else sparql""
      val query =
        if (countOnly)
          sparql"""
      SELECT (COUNT(*) AS ?count)
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      WHERE {
        SELECT DISTINCT ?phenotype ?phenotype_label 
        $whereClause
      }
      """
        else
          sparql"""
      SELECT DISTINCT ?phenotype ?phenotype_label
      FROM $KBMainGraph
      FROM $KBClosureGraph
      $namedQueriesBlock
      $whereClause
      ORDER BY LCASE(?phenotype_label) ?phenotype
      $paging
      """
      BlazegraphNamedSubquery.updateReferencesFor(unifiedQueries, query.text)
    }
  }

  //TODO extract common parts with TaxaWithPhenotype
  private def constructWhereClause(
      entity: Option[IRI],
      quality: QualitySpec,
      inTaxonOpt: Option[IRI],
      phenotypeOpt: Option[IRI],
      publicationOpt: Option[IRI],
      includeParts: Boolean,
      includeHistoricalHomologs: Boolean,
      includeSerialHomologs: Boolean
  ): Future[(QueryText, Set[BlazegraphNamedSubquery])] = {
    val validHomologyRelation = (if (includeHistoricalHomologs)
                                   Set(homologous_to.getIRI)
                                 else
                                   Set.empty) ++ (if (includeSerialHomologs)
                                                    Set(
                                                      serially_homologous_to.getIRI
                                                    )
                                                  else Set.empty)
    val homologyQueryPartsFut
        : ListT[Future, (List[QueryText], Set[BlazegraphNamedSubquery])] = for {
      entityTerm <- entity.toList |> Future.successful |> ListT.apply
      if includeHistoricalHomologs || includeSerialHomologs
      annotations <- AnatomicalEntity
        .homologyAnnotations(entityTerm, true)
        .map(List(_)) |> ListT.apply
      uniquedPositiveAnnotations = annotations
        .filterNot(_.negated)
        .map(ann => (ann.`object`, ann.objectTaxon, ann.relation))
        .toSet
      (otherEntity, otherTaxon, relation) <- uniquedPositiveAnnotations.toList |> Future.successful |> ListT.apply
      if validHomologyRelation(relation)
    } yield {
      var homComponents = List.empty[QueryText]
      var homSubqueries = Set.empty[BlazegraphNamedSubquery]
      val homSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(
        Option(otherEntity),
        quality,
        phenotypeOpt,
        false
      )
      val basicHom = coreTaxonToPhenotype(
        inTaxonOpt.toSet + otherTaxon,
        publicationOpt,
        homSubquery
      )
      homComponents = basicHom :: homComponents
      homSubquery.foreach(q => homSubqueries += q)
      if (includeParts) {
        val homPartsSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(
          Option(otherEntity),
          quality,
          phenotypeOpt,
          true
        )
        val homParts = coreTaxonToPhenotype(
          inTaxonOpt.toSet + otherTaxon,
          publicationOpt,
          homPartsSubquery
        )
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
      val basicSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(
        entity,
        quality,
        phenotypeOpt,
        false
      )
      val basic =
        coreTaxonToPhenotype(inTaxonOpt.toSet, publicationOpt, basicSubquery)
      components = basic :: components
      basicSubquery.foreach(q => subqueries += q)
      if (includeParts) {
        val partsSubquery = TaxaWithPhenotype.phenotypeSubQueryFor(
          entity,
          quality,
          phenotypeOpt,
          true
        )
        val parts =
          coreTaxonToPhenotype(inTaxonOpt.toSet, publicationOpt, partsSubquery)
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

  private def coreTaxonToPhenotype(
      inTaxa: Set[IRI],
      publicationOpt: Option[IRI],
      phenotypeQueries: Set[BlazegraphNamedSubquery]
  ): QueryText = {
    val taxonConstraints =
      (for { taxon <- inTaxa } yield sparql"?taxon $rdfsSubClassOf $taxon . ")
        .fold(sparql"")(_ |+| _)
    val subQueryRefs = QueryText(
      phenotypeQueries.map(q => sparql"$q").map(_.text).mkString("\n")
    )
    val publicationVal = publicationOpt
      .map(pub => sparql"VALUES ?matrix {  $pub }")
      .getOrElse(sparql"")
    sparql"""
      {
      $publicationVal
      ?taxon $exhibits_state ?state .
      ?state $describes_phenotype ?phenotype .
      ?phenotype $RDFSLabel ?phenotype_label .
      ?matrix $has_character/$may_have_state_value ?state .
      $taxonConstraints
      $subQueryRefs
      }
    """
  }

}
