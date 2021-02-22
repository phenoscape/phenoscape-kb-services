package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.query.Query
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.owl.Vocab.{rdfType}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolation.QueryText
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.scowl
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.semanticweb.owlapi.model.IRI

import scala.concurrent.Future
import scala.language.postfixOps

object Graph {

  def propertyNeighborsForObject(term: IRI, property: IRI): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQueryObject(term, property), MinimalTerm.fromQuerySolution)

  def propertyNeighborsForSubject(term: IRI, property: IRI, direct: Boolean): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQuerySubject(term, property, direct), MinimalTerm.fromQuerySolution)

  private def buildPropertyNeighborsQueryObject(focalTerm: IRI, property: IRI): Query = {
    val classRelation = NamedRestrictionGenerator.getClassRelationIRI(property)
    select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where bgp(
      t('existential_node, classRelation, focalTerm),
      t('existential_subclass, rdfsSubClassOf, 'existential_node),
      t('existential_subclass, classRelation, 'term),
      t('term, rdfsLabel, 'term_label)
    )
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

  def ancestorMatrix(terms: Set[IRI]): Future[AncestorMatrix] = {
    import scalaz._
    import Scalaz._
    if (terms.isEmpty) Future.successful(AncestorMatrix(""))
    else {
      val valuesElements = terms.map(t => sparql" $t ").reduce(_ + _)
      val query =
        sparql"""
       SELECT DISTINCT ?term ?ancestor
       FROM $KBClosureGraph
       WHERE {
         VALUES ?term { $valuesElements }
         ?term $rdfsSubClassOf ?ancestor .
         FILTER(?ancestor != $owlThing)
       }
          """
      val futurePairs = App.executeSPARQLQueryString(query.text,
                                                     qs => {
                                                       val term = qs.getResource("term").getURI
                                                       val ancestor = qs.getResource("ancestor").getURI
                                                       (term, ancestor)
                                                     })
      for {
        pairs <- futurePairs
      } yield {
        val termsSequence = terms.map(_.toString).toSeq.sorted
        val header = s",${termsSequence.mkString(",")}"
        val groupedByAncestor = pairs.groupBy(_._2)
        val valuesLines = groupedByAncestor.map {
          case (ancestor, ancPairs) =>
            val termsForAncestor = ancPairs.map(_._1).toSet
            val values = termsSequence.map(t => if (termsForAncestor(t)) "1" else "0")
            s"$ancestor,${values.mkString(",")}"
        }
        AncestorMatrix(s"$header\n${valuesLines.mkString("\n")}")
      }
    }
  }

  final case class AncestorMatrix(csv: String)

  object AncestorMatrix {

    implicit val matrixMarshaller: ToEntityMarshaller[AncestorMatrix] =
      Marshaller.stringMarshaller(MediaTypes.`text/csv`).compose(_.csv)

  }

}
