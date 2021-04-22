package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.query.Query
import org.apache.jena.sparql.path.Path
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.owl.Vocab.rdfType
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolation.QueryText
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.scowl
import org.phenoscape.sparql.SPARQLInterpolationOWL._

import scala.concurrent.Future
import scala.language.postfixOps
import java.net.{URLDecoder, URLEncoder}

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

  def ancestorMatrix(terms: Set[IRI], relations: Set[IRI], pathOpt: Option[Path]): Future[AncestorMatrix] =
    if (terms.isEmpty) Future.successful(AncestorMatrix(""))
    else {
      val termsElements = terms.map(t => sparql" $t ").reduceOption(_ + _).getOrElse(sparql"")
      val relationsElements = relations.map(r => sparql" $r ").reduceOption(_ + _).getOrElse(sparql"")
      val queryPattern = pathOpt match {
        case Some(path) => sparql""" ?term $path ?class . ?class ?relation ?subsumer ."""
        case None       => sparql""" ?term ?relation ?subsumer . """
      }

      val query =
        sparql"""
       SELECT DISTINCT ?term ?relation ?subsumer 
       FROM $KBClosureGraph
       FROM $KBRedundantRelationGraph
       WHERE {
         VALUES ?term { $termsElements }
         VALUES ?relation { $relationsElements }
         $queryPattern
         FILTER(?subsumer != $owlThing)
       }
          """

      print(" \n ***" + query.text)

      val futurePairs = App.executeSPARQLQueryString(
        query.text,
        qs => {
          val term = qs.getResource("term").getURI
          val relation = qs.getResource("relation").getURI
          val subsumer = qs.getResource("subsumer").getURI
          (term, relation, subsumer)
        }
      )
      for {
        pairs <- futurePairs
      } yield {
        val termsSequence = terms.map(_.toString).toSeq.sorted
        val header = s",${termsSequence.mkString(",")}"
        val termSubsumerPairs = pairs.map(p => (p._1 -> (p._2, p._3)))

        // creates Map(term -> virtualIRI(relation, subsumer))
        val termToRelSubsumerSeq = termSubsumerPairs.map { case (term, pairs) =>
          val virtualTermIRI = pairs._1 match {
            case "http://www.w3.org/2000/01/rdf-schema#subClassOf" => IRI.create(pairs._2)
            case _                                                 => RelationalTerm(IRI.create(pairs._1), IRI.create(pairs._2)).iri
          }

          Map(term -> virtualTermIRI)
        }.flatten

        val groupedBySubsumer = termToRelSubsumerSeq.groupBy(_._2)
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
