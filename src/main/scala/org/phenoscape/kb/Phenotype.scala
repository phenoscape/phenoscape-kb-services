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
import org.phenoscape.owl.Vocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.IRI
import com.google.common.collect.HashMultiset
import akka.util.Timeout
import org.phenoscape.kb.queries.QueryUtil.{PhenotypicQuality, QualitySpec}
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}

object Phenotype {

  implicit val timeout = Timeout(10 minutes)

  private val has_part_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.has_part.getIRI)
  private val phenotype_of_some: IRI = NamedRestrictionGenerator.getClassRelationIRI(Vocab.phenotype_of.getIRI)
  private val has_part_inhering_in_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.has_part_inhering_in.getIRI)

  def info(phenotype: IRI): Future[Phenotype] = {
    val eqsFuture = eqForPhenotype(phenotype)
    val statesFuture = characterStatesForPhenotype(phenotype)
    val labelFuture = Term.label(phenotype)
    for {
      eqs <- eqsFuture
      states <- statesFuture
      labelOpt <- labelFuture
    } yield Phenotype(phenotype, labelOpt.map(_.label).getOrElse(""), states, eqs)
  }

  def characterStatesForPhenotype(phenotype: IRI): Future[Set[CharacterState]] = {
    val query: QueryText =
      sparql"""
       SELECT DISTINCT ?character ?character_label ?state ?state_label ?matrix ?matrix_label
       FROM $KBMainGraph
       WHERE {
          ?state $describes_phenotype $phenotype .
          ?character $may_have_state_value ?state .
          ?matrix $has_character ?character .
          ?state $rdfsLabel ?state_label .
          ?character $rdfsLabel ?character_label .
          ?matrix $rdfsLabel ?matrix_label .
       }
            """
    App.executeSPARQLQueryString(query.text, solution => CharacterState(
      IRI.create(solution.getResource("state").getURI),
      solution.getLiteral("state_label").getLexicalForm,
      MinimalTerm(
        IRI.create(solution.getResource("character").getURI),
        solution.getLiteral("character_label").getLexicalForm,
      ),
      MinimalTerm(
        IRI.create(solution.getResource("matrix").getURI),
        solution.getLiteral("matrix_label").getLexicalForm
      )
    )).map(_.toSet)
  }

  def eqForPhenotype(phenotype: IRI): Future[NearestEQSet] = {
    val entitiesFuture = entitiesForPhenotype(phenotype, has_part_inhering_in_some)
    val generalEntitiesFuture = entitiesForPhenotype(phenotype, phenotype_of_some) //FIXME need to change to relatedEntities using has_part_towards_some; this must be added to the KB build
    //val relatedEntitiesFuture = ???
    val qualitiesFuture = qualitiesForPhenotype(phenotype)
    for {
      entities <- entitiesFuture
      generalEntities <- generalEntitiesFuture
      qualities <- qualitiesFuture
    } yield NearestEQSet(entities, qualities, generalEntities -- entities) //FIXME this is an approximation for using towards
  }

  def entitiesForPhenotype(phenotype: IRI, relation: IRI): Future[Set[IRI]] = {
    for {
      entityTypesResult <- App.executeSPARQLQuery(entitySuperClassesQuery(phenotype, relation), result => IRI.create(result.getResource("description").getURI))
      entitySuperClasses <- superClassesForEntityTypes(entityTypesResult, relation)
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

  def queryTaxonPhenotypes(entity: Option[IRI], quality: QualitySpec, inTaxonOpt: Option[IRI], phenotypeOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, limit: Int = 20, offset: Int = 0): Future[Seq[MinimalTerm]] = for {
    query <- TaxonPhenotypes.buildQuery(entity, quality, inTaxonOpt, phenotypeOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, false, limit, offset)
    _ = println(query)
    phenotypes <- App.executeSPARQLQueryString(query, fromQueryResult)
  } yield phenotypes

  def queryTaxonPhenotypesTotal(entity: Option[IRI], quality: QualitySpec, inTaxonOpt: Option[IRI], phenotypeOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Int] = for {
    query <- TaxonPhenotypes.buildQuery(entity, quality, inTaxonOpt, phenotypeOpt, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
    result <- App.executeSPARQLQuery(query)
  } yield ResultCount.count(result)

  def facetPhenotypeByEntity(focalEntity: Option[IRI], quality: QualitySpec, inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryTaxonPhenotypesTotal(Some(iri), quality, inTaxonOpt, None, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.queryAnatomySubClasses(iri, KBVocab.Uberon, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine, false)
  }

  def facetPhenotypeByQuality(focalQuality: Option[IRI], entity: Option[IRI], inTaxonOpt: Option[IRI], publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryTaxonPhenotypesTotal(entity, PhenotypicQuality(Some(iri)), inTaxonOpt, None, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine, false)
  }

  def facetPhenotypeByTaxon(focalTaxon: Option[IRI], entity: Option[IRI], quality: QualitySpec, publicationOpt: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => queryTaxonPhenotypesTotal(entity, quality, Some(iri), None, publicationOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.VTO)).map(_.toSet)
    Facets.facet(focalTaxon.getOrElse(KBVocab.taxonRoot), query, refine, true)
  }

  private def fromQueryResult(result: QuerySolution): MinimalTerm = MinimalTerm(
    IRI.create(result.getResource("phenotype").getURI),
    result.getLiteral("phenotype_label").getLexicalForm)

  private def entitySuperClassesQuery(phenotype: IRI, relation: IRI): Query =
    select_distinct('description) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (
      bgp(
        t(phenotype, rdfsSubClassOf, 'description),
        t('description, relation, 'entity),
        t('entity, rdfsIsDefinedBy, Uberon)))

  private def entityEntitySuperClassesQuery(phenotype: IRI, relation: IRI): Query =
    select_distinct('entity) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (
      bgp(
        t(phenotype, rdfsSubClassOf, 'description),
        t('description, relation, 'entity),
        t('entity, rdfsIsDefinedBy, Uberon)))

  private def superClassesForEntityTypes(entityTypes: Iterable[IRI], relation: IRI): Future[Iterable[IRI]] = {
    val futureSuperclasses = Future.sequence(entityTypes.map { entityType =>
      App.executeSPARQLQuery(entityEntitySuperClassesQuery(entityType, relation), result => IRI.create(result.getResource("entity").getURI))
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

final case class Phenotype(iri: IRI, label: String, states: Set[CharacterState], eqs: NearestEQSet) extends JSONResultItem {

  override def toJSON: JsObject =
    Map(
      "@id" -> iri.toString.toJson,
      "label" -> label.toJson,
      "states" -> states.map(_.toJSON).toJson,
      "eqs" -> eqs.toJSON
    ).toJson.asJsObject

}

final case class NearestEQSet(entities: Set[IRI], qualities: Set[IRI], relatedEntities: Set[IRI]) extends JSONResultItem {

  override def toJSON: JsObject =
    Map(
      "entities" -> entities.map(_.toString).toSeq.sorted.toJson,
      "qualities" -> qualities.map(_.toString).toSeq.sorted.toJson,
      "related_entities" -> relatedEntities.map(_.toString).toSeq.sorted.toJson
    ).toJson.asJsObject

}