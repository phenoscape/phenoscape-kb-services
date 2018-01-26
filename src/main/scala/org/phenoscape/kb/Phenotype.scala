package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.phenoscape.kb.Facets.Facet
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.queries.TaxonPhenotypes
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI

import com.google.common.collect.HashMultiset

import akka.util.Timeout
import spray.json._
import spray.json.DefaultJsonProtocol._

object Phenotype {

  implicit val timeout = Timeout(10 minutes)

  private val has_part_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.has_part.getIRI)
  private val phenotype_of_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.phenotype_of.getIRI)

  def eqForPhenotype(phenotype: IRI): Future[JsObject] = {
    val entitiesFuture = entitiesForPhenotype(phenotype)
    val qualitiesFuture = qualitiesForPhenotype(phenotype)
    for {
      entities <- entitiesFuture
      qualities <- qualitiesFuture
    } yield Map(
      "entity" -> entities.map(_.toString).toSeq.sorted.toJson,
      "quality" -> qualities.map(_.toString).toSeq.sorted.toJson).toJson.asJsObject
  }

  def entitiesForPhenotype(phenotype: IRI): Future[Set[IRI]] = {
    for {
      entityTypesResult <- App.executeSPARQLQuery(entitySuperClassesQuery(phenotype), result => IRI.create(result.getResource("description").getURI))
      entitySuperClasses <- superClassesForEntityTypes(entityTypesResult)
    } yield {
      val superclasses = HashMultiset.create[IRI]
      entitySuperClasses.foreach(superclasses.add)
      superclasses.entrySet.filter(_.getCount == 1).map(_.getElement).toSet
    }
  }

  def qualitiesForPhenotype(phenotype: IRI): Future[Set[IRI]] = {
    for {
      superQualities <- App.executeSPARQLQuery(qualitySuperClasses(phenotype), result => IRI.create(result.getResource("description").getURI))
      superSuperQualities <- superClassesForSuperQualities(superQualities)
    } yield {
      val superclasses = HashMultiset.create[IRI]
      superSuperQualities.foreach(superclasses.add)
      superclasses.entrySet.filter(_.getCount == 1).map(_.getElement).toSet
    }
  }

  def queryTaxonPhenotypes(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, limit: Int = 20, offset: Int = 0): Future[Seq[MinimalTerm]] = for {
    query <- TaxonPhenotypes.buildQuery(entity, quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, false, limit, offset)
    _ = println(query)
    phenotypes <- App.executeSPARQLQueryString(query, fromQueryResult)
  } yield phenotypes

  def queryTaxonPhenotypesTotal(entity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Int] = for {
    query <- TaxonPhenotypes.buildQuery(entity, quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def facetPhenotypeByEntity(focalEntity: Option[IRI], quality: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryTaxonPhenotypesTotal(Some(iri), quality, inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.Uberon)).map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine)
  }

  def facetPhenotypeByQuality(focalQuality: Option[IRI], entity: Option[IRI], inTaxonOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryTaxonPhenotypesTotal(entity, Some(iri), inTaxonOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine)
  }

  def facetPhenotypeByTaxon(focalTaxon: Option[IRI], entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryTaxonPhenotypesTotal(entity, quality, Some(iri), includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.VTO)).map(_.toSet)
    Facets.facet(focalTaxon.getOrElse(KBVocab.taxonRoot), query, refine)
  }

  private def fromQueryResult(result: QuerySolution): MinimalTerm = MinimalTerm(
    IRI.create(result.getResource("phenotype").getURI),
    result.getLiteral("phenotype_label").getLexicalForm)

  private def entitySuperClassesQuery(phenotype: IRI): Query =
    select_distinct('description) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (
      bgp(
        t(phenotype, rdfsSubClassOf, 'description),
        t('description, phenotype_of_some, 'entity),
        t('entity, rdfsIsDefinedBy, Uberon)))

  private def entityEntitySuperClassesQuery(phenotype: IRI): Query =
    select_distinct('entity) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (
      bgp(
        t(phenotype, rdfsSubClassOf, 'description),
        t('description, phenotype_of_some, 'entity),
        t('entity, rdfsIsDefinedBy, Uberon)))

  private def superClassesForEntityTypes(entityTypes: Iterable[IRI]): Future[Iterable[IRI]] = {
    val futureSuperclasses = Future.sequence(entityTypes.map { entityType =>
      App.executeSPARQLQuery(entityEntitySuperClassesQuery(entityType), result => IRI.create(result.getResource("entity").getURI))
    })
    futureSuperclasses.map(_.flatten)
  }

  private def superClassesForSuperQualities(superQualities: Iterable[IRI]): Future[Iterable[IRI]] = {
    val futureSuperclasses = Future.sequence(superQualities.map { superClass =>
      App.executeSPARQLQuery(qualityQualitySuperClasses(superClass), result => IRI.create(result.getResource("quality").getURI))
    })
    futureSuperclasses.map(_.flatten)
  }

  private def qualitySuperClasses(phenotype: IRI): Query =
    select_distinct('description) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (
      bgp(
        t(phenotype, rdfsSubClassOf, 'description),
        t('description, has_part_some, 'quality),
        t('quality, rdfsIsDefinedBy, PATO)))

  private def qualityQualitySuperClasses(phenotype: IRI): Query =
    select_distinct('quality) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (
      bgp(
        t(phenotype, rdfsSubClassOf, 'description),
        t('description, has_part_some, 'quality),
        t('quality, rdfsIsDefinedBy, PATO)))

}