package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.parsing.json.JSONArray
import scala.util.parsing.json.JSONObject
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.language.postfixOps

import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.KBVocab._
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI

import com.google.common.collect.HashMultiset
import com.hp.hpl.jena.query.Query
import akka.util.Timeout
import scala.concurrent.duration._

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