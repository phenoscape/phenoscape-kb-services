package org.phenoscape.kb

import akka.util.Timeout
import org.apache.jena.query.Query
import org.phenoscape.kb.KBVocab._
import org.phenoscape.owl.Vocab._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object EQForGene {

  implicit private val timeout: Timeout = Timeout(10 minutes)

  def query(geneID: IRI): Future[JsArray] = {
    val result = for {
      annotations <- annotationsForGene(geneID)
    } yield {
      val allAnnotationsFuture = Future.sequence(
        for {
          annotationID <- annotations
        } yield {
          val entitiesFuture = entitiesForAnnotation(annotationID)
          val qualitiesFuture = qualitiesForAnnotation(annotationID)
          for {
            entities <- entitiesFuture
            qualities <- qualitiesFuture
          } yield Map("entity" -> entities, "quality" -> qualities).toJson
        }
      )
      allAnnotationsFuture.map(annotations => JsArray(annotations.toVector))
    }
    result.flatMap(identity) //FIXME this method is a bit messy
  }

  def annotationsForGene(geneID: IRI): Future[Iterable[String]] =
    App.executeSPARQLQuery(annotationsQuery(geneID), _.getResource("association").getURI)

  def annotationsQuery(geneIRI: IRI): Query =
    sparql"""
      SELECT DISTINCT ?association
      FROM $KBMainGraph
      WHERE {
        ?association $rdfType $association .
        ?association $associationHasPredicate $has_phenotype .
        ?association $associationHasSubject $geneIRI .
      }
    """.toQuery

  def qualitiesForAnnotation(annotationID: String): Future[Iterable[String]] =
    App.executeSPARQLQuery(annotationSuperQualityQuery(annotationID), _.getResource("quality").getURI)

  def annotationSuperQualityQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)

    val query =
      sparql"""
          SELECT DISTINCT ?quality
          FROM $KBMainGraph
          FROM $KBRedundantRelationGraph
          WHERE {
            $annotationIRI $associationHasObject ?phenotype .
            ?phenotype $has_part ?quality .
            ?quality $rdfsIsDefinedBy $PATO .
            
            FILTER NOT EXISTS {
              ?phenotype  $has_part ?other_quality .
              ?other_quality $rdfsIsDefinedBy $PATO .
              ?other_quality ${KBVocab.rdfsSubClassOf} ?quality .
              FILTER (?other_quality != ?quality)
            }
          }
          """
    query.toQuery
  }

  def entitiesForAnnotation(annotationID: String): Future[Iterable[String]] =
    App.executeSPARQLQuery(annotationEntityTypesQuery(annotationID), _.getResource("bearer").getURI)

  def annotationEntityTypesQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    val query =
      sparql"""
          SELECT DISTINCT ?bearer
          FROM $KBMainGraph
          FROM $KBRedundantRelationGraph
          WHERE {
            $annotationIRI $associationHasObject ?phenotype .

            ?phenotype $has_part_inhering_in ?bearer .
            ?bearer $rdfsIsDefinedBy  $Uberon .

            FILTER NOT EXISTS {
              ?phenotype  $has_part_inhering_in ?other_bearer .
              ?other_bearer $rdfsIsDefinedBy  $Uberon .
              ?other_bearer ${KBVocab.rdfsSubClassOf} ?bearer .
              FILTER (?other_bearer != ?bearer)
            }
          }
          """
    query.toQuery

  }

}
