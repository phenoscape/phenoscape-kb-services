package org.phenoscape.kb.resource

import javax.ws.rs.Path
import javax.ws.rs.Produces
import java.io.BufferedWriter
import javax.ws.rs.core.StreamingOutput
import javax.ws.rs.GET
import javax.ws.rs.client.ClientBuilder
import java.io.OutputStream
import javax.ws.rs.core.Response
import java.io.OutputStreamWriter
import org.phenoscape.kb.util.App
import javax.ws.rs.QueryParam
import org.phenoscape.scowl.OWL._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import com.hp.hpl.jena.query.Query
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.apibinding.OWLManager
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP
import scala.collection.JavaConversions._
import com.google.common.collect.HashMultiset
import org.phenoscape.owl.NamedRestrictionGenerator
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.util.parsing.json.JSONArray
import scala.util.parsing.json.JSONObject
import scala.concurrent.ExecutionContext

@Path("gene/eq")
class EQForGene(@QueryParam("id") var geneIDParam: String) {

  private val geneIDOption: Option[String] = Option(geneIDParam)
  val factory = OWLManager.getOWLDataFactory
  val rdfType = ObjectProperty(Vocab.rdfType)
  val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
  val rdfsIsDefinedBy = factory.getRDFSIsDefinedBy
  val inheresInSome = NamedRestrictionGenerator.getClassRelationIRI(Vocab.inheres_in.getIRI)
  val UBERON = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")
  val PATO = IRI.create("http://purl.obolibrary.org/obo/pato.owl")

  @GET
  @Produces(Array("application/json"))
  def query(): String = {
    val geneID = geneIDOption.getOrElse(???)
    val annotations = annotationsForGene(geneID)
    val allAnnotationsFuture = Future.sequence(for {
      annotationID <- annotations
    } yield {
      val entitiesFuture = entitiesForAnnotation(annotationID)
      val qualitiesFuture = qualitiesForAnnotation(annotationID)
      for {
        entities <- entitiesFuture
        qualities <- qualitiesFuture
      } yield new JSONObject(Map("entity" -> new JSONArray(entities.toList), "quality" -> new JSONArray(qualities.toList)))
    })
    new JSONArray(Await.result(allAnnotationsFuture, 10 minutes).toList).toString
  }

  def annotationsForGene(geneID: String): Iterable[String] = {
    val queryEngine = new QueryEngineHTTP(App.endpoint, annotationsQuery(geneID))
    val resultSet = queryEngine.execSelect
    val annotations = (resultSet map { item => item.getResource("annotation").getURI }).toList
    queryEngine.close()
    annotations
  }

  def annotationsQuery(geneID: String): Query = {
    val geneIRI = IRI.create(geneID)
    select_distinct('annotation) from "http://kb.phenoscape.org/" where (
      bgp(
        t('annotation, rdfType, Vocab.AnnotatedPhenotype),
        t('annotation, Vocab.associated_with_gene, geneIRI)))
  }

  def qualitiesForAnnotation(annotationID: String): Future[Iterable[String]] = {
    val allSuperQualities = Future {
      blocking {
        val queryEngine = new QueryEngineHTTP(App.endpoint, annotationSuperQualityQuery(annotationID))
        val resultSet = queryEngine.execSelect
        val results = (resultSet map (_.getResource("quality").getURI)).toList
        queryEngine.close()
        results
      }
    }
    for {
      superQualities <- allSuperQualities
      superSuperQualities <- superClassesForSuperQualities(superQualities)
    } yield {
      val superclasses = HashMultiset.create[String]
      superSuperQualities foreach (superclasses.add(_))
      val nearestQualities = superclasses.entrySet filter (_.getCount == 1) map (_.getElement)
      nearestQualities.toList
    }
  }

  def superClassesForSuperQualities(superQualities: Iterable[String]): Future[Iterable[String]] = {
    val superclasses = Future.sequence(superQualities map { superClass =>
      Future {
        blocking {
          val subquery = qualitySuperQualityQuery(superClass)
          val subqueryEngine = new QueryEngineHTTP(App.endpoint, subquery);
          val subResultSet = subqueryEngine.execSelect
          val results = (subResultSet map (_.getResource("quality").getURI)).toList
          subqueryEngine.close()
          results
        }
      }
    })
    for { result <- superclasses } yield result.flatten
  }

  def annotationSuperQualityQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    select_distinct('quality) from "http://kb.phenoscape.org/" where (
      bgp(
        t(annotationIRI, rdfType / (rdfsSubClassOf*), 'quality),
        t('quality, rdfsIsDefinedBy, PATO)))
  }

  def qualitySuperQualityQuery(termID: String): Query = {
    val termIRI = IRI.create(termID)
    select_distinct('quality) from "http://kb.phenoscape.org/" where (
      bgp(
        t(termIRI, rdfsSubClassOf*, 'quality),
        t('quality, rdfsIsDefinedBy, PATO)))
  }

  def entitiesForAnnotation(annotationID: String): Future[Iterable[String]] = {
    val entityTypes = Future {
      blocking {
        val queryEngine = new QueryEngineHTTP(App.endpoint, annotationEntityTypesQuery(annotationID))
        val resultSet = queryEngine.execSelect
        val results = (resultSet map (_.getResource("description").getURI)).toList
        queryEngine.close()
        results
      }
    }
    for {
      entityTypesResult <- entityTypes
      entitySuperClasses <- superClassesForEntityTypes(entityTypesResult)
    } yield {
      val superclasses = HashMultiset.create[String]
      entitySuperClasses foreach (superclasses.add(_))
      val nearestEntities = superclasses.entrySet filter (_.getCount == 1) map (_.getElement)
      nearestEntities.toList
    }
  }

  def superClassesForEntityTypes(entityTypes: Iterable[String]): Future[Iterable[String]] = {
    val superclasses = Future.sequence(entityTypes map { entityType =>
      Future {
        blocking {
          val subquery = entitySuperClassesQuery(entityType)
          val subqueryEngine = new QueryEngineHTTP(App.endpoint, subquery);
          val subResultSet = subqueryEngine.execSelect
          val results = (subResultSet map (_.getResource("bearer").getURI)).toList
          subqueryEngine.close()
          results
        }
      }
    })
    for { result <- superclasses } yield result.flatten
  }

  def annotationEntityTypesQuery(annotationID: String): Query = {
    val annotationIRI = IRI.create(annotationID)
    select_distinct('description) from "http://kb.phenoscape.org/" where (
      bgp(
        t(annotationIRI, rdfType / (rdfsSubClassOf*), 'description),
        t('description, inheresInSome, 'bearer),
        t('bearer, rdfsIsDefinedBy, UBERON)))
  }

  def entitySuperClassesQuery(termID: String): Query = {
    val termIRI = IRI.create(termID)
    select_distinct('bearer) from "http://kb.phenoscape.org/" where (
      bgp(
        t(termIRI, rdfsSubClassOf*, 'description),
        t('description, inheresInSome, 'bearer),
        t('bearer, rdfsIsDefinedBy, UBERON)))
  }

}