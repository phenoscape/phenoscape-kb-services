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

import scala.concurrent.Future
import scala.language.postfixOps

object Graph {

  def propertyNeighborsForObject(term: IRI, property: IRI, direct: Boolean): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQueryObject(term, property, direct), MinimalTerm.fromQuerySolution)

  def propertyNeighborsForSubject(term: IRI, property: IRI, direct: Boolean): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQuerySubject(term, property, direct), MinimalTerm.fromQuerySolution)

  private def buildPropertyNeighborsQueryObject(focalTerm: IRI, property: IRI, direct: Boolean): Query = {

    val filterIndirectNeighbors =
      sparql"""
              FILTER NOT EXISTS {
                  ?other_term $property $focalTerm .
                  ?other_term $rdfsSubClassOf ?term .
                  FILTER(?other_term != ?term)
                }
              
              FILTER NOT EXISTS {
                  $property $rdfType $transitiveProperty .
                  ?other_term $property $focalTerm .
                  ?other_term $property ?term .
                  FILTER(?other_term != ?term)
                }
            """

    val filters = if (direct) filterIndirectNeighbors else sparql""

    val query =
      sparql"""
              SELECT DISTINCT ?term ?term_label
              FROM $KBMainGraph
              FROM $KBClosureGraph
              FROM $KBRedundantRelationGraph
              WHERE {
                ?term  $property $focalTerm  .
                ?term $rdfsLabel ?term_label .

                $filters
              }
              """

    query.toQuery
  }

  private def buildPropertyNeighborsQuerySubject(focalTerm: IRI, property: IRI, direct: Boolean): Query = {

    val filterIndirectNeighbors =
      sparql"""
              FILTER NOT EXISTS {
                  $focalTerm $property ?other_term .
                  ?other_term $rdfsSubClassOf ?term .
                  FILTER(?other_term != ?term)
                }
              
              FILTER NOT EXISTS {
                  $property $rdfType $transitiveProperty .
                  $focalTerm $property ?other_term .
                  ?other_term $property ?term .
                  FILTER(?other_term != ?term)
                }
            """

    val filters = if (direct) filterIndirectNeighbors else sparql""

    val query =
      sparql"""
              SELECT DISTINCT ?term ?term_label
              FROM $KBMainGraph
              FROM $KBClosureGraph
              FROM $KBRedundantRelationGraph
              WHERE {
              $focalTerm $property ?term .
              ?term $rdfsLabel ?term_label .
              
              $filters
              }
              """

    query.toQuery
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
