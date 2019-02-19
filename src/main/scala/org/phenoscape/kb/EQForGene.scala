package org.phenoscape.kb

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.jena.query.Query
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI

import com.google.common.collect.HashMultiset

import akka.util.Timeout
import spray.json._
import spray.json.DefaultJsonProtocol._

object EQForGene {

  private val factory = OWLManager.getOWLDataFactory
  private val rdfType = ObjectProperty(Vocab.rdfType)
  private val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
  private val rdfsIsDefinedBy = factory.getRDFSIsDefinedBy
  private val UBERON = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")
  private val PATO = IRI.create("http://purl.obolibrary.org/obo/pato.owl")
  private val has_part_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.has_part.getIRI)
  private val has_part_inhering_in_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.has_part_inhering_in.getIRI)
  private implicit val timeout: Timeout = Timeout(10 minutes)

  def query(geneID: IRI): Future[JsArray] = {
    val result = for {
      annotations <- annotationsForGene(geneID)
    } yield {
      val allAnnotationsFuture = Future.sequence(for {
        annotationID <- annotations
      } yield {
        val entitiesFuture = entitiesForAnnotation(annotationID)
        val qualitiesFuture = qualitiesForAnnotation(annotationID)
        for {
          entities <- entitiesFuture
          qualities <- qualitiesFuture
        } yield Map("entity" -> entities, "quality" -> qualities).toJson
      })
      allAnnotationsFuture.map(annotations => JsArray(annotations.toVector))
    }
    result.flatMap(identity) //FIXME this method is a bit messy
  }

  def annotationsForGene(geneID: IRI): Future[Iterable[String]] = {
    App.executeSPARQLQuery(annotationsQuery(geneID), _.getResource("annotation").getURI)
  }

  def annotationsQuery(geneIRI: IRI): Query = {
    select_distinct('annotation) from "http://kb.phenoscape.org/" where bgp(
      t('annotation, rdfType, Vocab.AnnotatedPhenotype),
      t('annotation, Vocab.associated_with_gene, geneIRI))
  }

  def qualitiesForAnnotation(annotationID: String): Future[Iterable[String]] = {
    val allSuperQualities = App.executeSPARQLQuery(annotationSuperQualityQuery(annotationID), _.getResource("quality").getURI)
    for {
      superQualities <- allSuperQualities
      superSuperQualities <- superClassesForSuperQualities(superQualities)
    } yield {
      val superclasses = HashMultiset.create[String]
      superSuperQualities.foreach(superclasses.add)
      val nearestQualities = superclasses.entrySet.asScala.filter(_.getCount == 1).map(_.getElement)
      nearestQualities.toVector
    }
  }

  def superClassesForSuperQualities(superQualities: Iterable[String]): Future[Iterable[String]] = {
    val superclasses = Future.sequence(superQualities.map { superClass =>
      App.executeSPARQLQuery(qualitySuperQualityQuery(superClass), _.getResource("quality").getURI)
    })
    for { result <- superclasses } yield result.flatten
  }

  def annotationSuperQualityQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    select_distinct('quality) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where bgp(
      t(annotationIRI, rdfType / rdfsSubClassOf, 'has_quality),
      t('has_quality, has_part_some, 'quality),
      t('quality, rdfsIsDefinedBy, PATO))
  }

  def qualitySuperQualityQuery(termID: String): Query = {
    val termIRI = IRI.create(termID)
    select_distinct('quality) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where bgp(
      t(termIRI, rdfsSubClassOf, 'has_quality),
      t('has_quality, has_part_some, 'quality),
      t('quality, rdfsIsDefinedBy, PATO))
  }

  def entitiesForAnnotation(annotationID: String): Future[Iterable[String]] = {
    val entityTypes = App.executeSPARQLQuery(annotationEntityTypesQuery(annotationID), _.getResource("description").getURI)
    for {
      entityTypesResult <- entityTypes
      entitySuperClasses <- superClassesForEntityTypes(entityTypesResult)
    } yield {
      val superclasses = HashMultiset.create[String]
      entitySuperClasses.foreach(superclasses.add)
      val nearestEntities = superclasses.entrySet.asScala.filter(_.getCount == 1).map(_.getElement)
      nearestEntities.toVector
    }
  }

  def superClassesForEntityTypes(entityTypes: Iterable[String]): Future[Iterable[String]] = {
    val superclasses = Future.sequence(entityTypes.map { entityType =>
      App.executeSPARQLQuery(entitySuperClassesQuery(entityType), _.getResource("bearer").getURI)
    })
    for { result <- superclasses } yield result.flatten
  }

  def annotationEntityTypesQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    select_distinct('description) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where bgp(
      t(annotationIRI, rdfType / rdfsSubClassOf, 'description),
      t('description, has_part_inhering_in_some, 'bearer),
      t('bearer, rdfsIsDefinedBy, UBERON))
  }

  def entitySuperClassesQuery(termID: String): Query = {
    val termIRI = IRI.create(termID)
    select_distinct('bearer) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where bgp(
      t(termIRI, rdfsSubClassOf, 'description),
      t('description, has_part_inhering_in_some, 'bearer),
      t('bearer, rdfsIsDefinedBy, UBERON))
  }

}