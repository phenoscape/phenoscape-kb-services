package org.phenoscape.kb.queries

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_Coalesce
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueString
import org.apache.jena.sparql.syntax.ElementBind
import org.apache.jena.sparql.syntax.ElementNamedGraph
import org.apache.jena.sparql.syntax.ElementSubQuery
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

object TaxaWithPhenotype {

  private val PhenotypeOfSome = NamedRestrictionGenerator.getClassRelationIRI(phenotype_of.getIRI)
  private val PartOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)
  private val HasPartSome = NamedRestrictionGenerator.getClassRelationIRI(has_part.getIRI)

  //
  //  private def buildBasicTaxaWithPhenotypeQuery(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Query] = {
  //    val actualEntity = (includeHistoricalHomologs, includeSerialHomologs) match {
  //      case (false, false) => entity
  //      case (true, false)  => entity or (homologous_to some entity)
  //      case (false, true)  => entity or (serially_homologous_to some entity)
  //      case (true, true)   => entity or (homologous_to some entity) or (serially_homologous_to some entity)
  //    }
  //    val entityExpression = if (includeParts) (actualEntity or (part_of some actualEntity)) else actualEntity
  //    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf*, _)).toList
  //    val query = select_distinct('taxon, 'taxon_label) where (
  //      bgp(
  //        App.BigdataAnalyticQuery ::
  //          t('state, describes_phenotype, 'phenotype) ::
  //          t('taxon, exhibits_state, 'state) ::
  //          t('taxon, rdfsLabel, 'taxon_label) ::
  //          t('phenotype, rdfsSubClassOf, ((has_part some quality) and (phenotype_of some entityExpression)).asOMN) ::
  //          taxonPatterns: _*))
  //    App.expandWithOwlet(query)
  //  }

  def buildQuery(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, countOnly: Boolean, limit: Int, offset: Int): String = {
    val (whereClause, subqueries) = constructWhereClause(entity, quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
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

  private def constructWhereClause(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): (QueryText, Set[BlazegraphNamedSubquery]) = {
    println(s"IN WHERE CLAUSE: $entity")
    var components = List.empty[QueryText]
    var subqueries = Set.empty[BlazegraphNamedSubquery]
    val basicSubquery = subQueryFor(entity, quality, false)
    val basic = coreTaxonToPhenotype(inTaxonOpt.toSet, basicSubquery)
    components = basic :: components
    basicSubquery.foreach(q => subqueries += q)
    if (includeParts) {
      val partsSubquery = subQueryFor(entity, quality, true)
      val parts = coreTaxonToPhenotype(inTaxonOpt.toSet, partsSubquery)
      components = parts :: components
      partsSubquery.foreach(q => subqueries += q)
    }
    //TODO homology
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

  private def subQueryFor(entity: Option[IRI], quality: Option[IRI], parts: Boolean): Option[BlazegraphNamedSubquery] = if (entity.nonEmpty || quality.nonEmpty) {
    println(s"SUBQUERY ENTITY: $entity")
    val entityPattern = entity.map { e =>
      if (parts) sparql"?p $rdfsSubClassOf/$PhenotypeOfSome/$PartOfSome $e . "
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