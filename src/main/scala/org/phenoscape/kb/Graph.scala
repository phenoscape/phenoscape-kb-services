package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.query.Query
import org.apache.jena.sparql.path.Path
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.owl.Vocab.rdfType
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Similarity.PhenotypeCorpus
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owlet.SPARQLComposer._

import scala.concurrent.Future
import scala.language.postfixOps

object Graph {

  def propertyNeighborsForObject(term: IRI, property: IRI): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQueryObject(term, property), MinimalTerm.fromQuerySolution)

  def propertyNeighborsForSubject(term: IRI, property: IRI): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQuerySubject(term, property), MinimalTerm.fromQuerySolution)

  private def buildPropertyNeighborsQueryObject(focalTerm: IRI, property: IRI): Query = {
    val classRelation = NamedRestrictionGenerator.getClassRelationIRI(property)
    select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where bgp(
      t('existential_node, classRelation, focalTerm),
      t('existential_subclass, rdfsSubClassOf, 'existential_node),
      t('existential_subclass, classRelation, 'term),
      t('term, rdfsLabel, 'term_label)
    )
  }

  private def buildPropertyNeighborsQuerySubject(focalTerm: IRI, property: IRI): Query = {
    val classRelation = NamedRestrictionGenerator.getClassRelationIRI(property)
    select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where bgp(
      t('existential_node, classRelation, focalTerm),
      t('existential_node, rdfsSubClassOf, 'existential_superclass),
      t('existential_superclass, classRelation, 'term),
      t('term, rdfsLabel, 'term_label)
    )
  }

  def getTermSubsumerPairs(terms: Set[IRI], corpusOpt: Option[PhenotypeCorpus]): Future[Seq[(IRI, IRI)]] = {
    val termsElements = terms.map(t => sparql" $t ").reduceOption(_ + _).getOrElse(sparql"")
    val pathOpt = corpusOpt.map(_.path)
    val queryPattern = pathOpt match {
      case Some(path) => sparql""" ?term $path ?class . ?class $rdfsSubClassOf ?subsumer ."""
      case None       => sparql""" ?term $rdfsSubClassOf ?subsumer . """
    }
    val query =
      sparql"""
       SELECT DISTINCT ?term ?subsumer 
       FROM $KBMainGraph
       FROM $KBClosureGraph
       WHERE {
         VALUES ?term { $termsElements }
         $queryPattern
         FILTER(?subsumer != $owlThing)
         FILTER(isIRI(?subsumer))
       }
       """
    App.executeSPARQLQueryString(
      query.text,
      qs => {
        val term = qs.getResource("term").getURI
        val subsumer = qs.getResource("subsumer").getURI
        IRI.create(term) -> IRI.create(subsumer)
      }
    )
  }

  def ancestorMatrix(terms: Set[IRI], corpusOpt: Option[PhenotypeCorpus]): Future[AncestorMatrix] =
    if (terms.isEmpty) Future.successful(AncestorMatrix(""))
    else {
      val termSubsumerPairsFut =
        getTermSubsumerPairs(terms, corpusOpt)
      for {
        termSubsumerPairs <- termSubsumerPairsFut
      } yield {
        val termsSequence = terms.toSeq.sortBy(_.toString)
        val header = s",${termsSequence.mkString(",")}"

        val groupedBySubsumer = termSubsumerPairs.groupBy(_._2)

        val valuesLines = groupedBySubsumer.map { case (subsumer, subsumerPairs) =>
          val termsForSubsumer = subsumerPairs.map(_._1).toSet
          val values = termsSequence.map(t => if (termsForSubsumer(t)) "1" else "0")
          s"$subsumer,${values.mkString(",")}"
        }

        AncestorMatrix(s"$header\n${valuesLines.mkString("\n")}")
      }
    }

  final case class AncestorMatrix(csv: String)

  object AncestorMatrix {

    implicit val matrixMarshaller: ToEntityMarshaller[AncestorMatrix] =
      Marshaller.stringMarshaller(MediaTypes.`text/csv`).compose(_.csv)

  }

}
