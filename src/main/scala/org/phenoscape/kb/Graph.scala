package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.query.Query
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
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

  def propertyNeighborsForSubject(term: IRI, property: IRI): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildPropertyNeighborsQuerySubject(term, property), MinimalTerm.fromQuerySolution)

  private def buildPropertyNeighborsQueryObject(focalTerm: IRI, property: IRI, direct: Boolean): Query = {

    val allNeighbors =
      sparql"""
              SELECT DISTINCT ?subject ?subject_label
              FROM $KBMainGraph
              FROM $KBClosureGraph
              FROM $KBRedundantRelationGraph
              WHERE {
                
                VALUES ?query_term { $focalTerm }
                VALUES ?relation { $property }
               
              ?subject  ?relation ?query_term  .
              ?subject rdfs:label ?subject_label .
              ?query_term rdfs:label ?label .
              """

    val filterIndirectNeighbors =
      sparql"""
              
              FILTER NOT EXISTS {
                  ?other_subject ?relation ?query_term .
                  ?other_subject rdfs:subClassOf ?subject .
                  FILTER(?other_subject != ?subject)
                }
              
              FILTER NOT EXISTS {
                  ?relation rdf:type owl:TransitiveProperty .
                  ?other_subject ?relation ?query_term .
                  ?other_subject ?relation ?subject .
                  FILTER(?other_subject != ?subject)
                }
              }
            """

    val query = sparql"$allNeighbors" + { if (direct) sparql"$filterIndirectNeighbors" else sparql"" }

    query.toQuery
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
