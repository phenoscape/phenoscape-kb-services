package org.phenoscape.kb

import scala.concurrent.Future
import org.semanticweb.owlapi.model.IRI
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.scowl._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import scalaz._
import spray.json._
import spray.json.DefaultJsonProtocol._

object Facets {

  val minimumSize = 5
  val maximumSize = 15

  type MoreSpecificFn = IRI => Future[Set[MinimalTerm]]

  type CountFn = IRI => Future[Int]

  def facet(focus: IRI, query: CountFn, refine: MoreSpecificFn): Future[List[Facet]] =
    for {
      partitions <- partition(focus, query, refine)
      expanded <- expandMax(partitions, query, refine)
      //deepened <- Future.sequence(expanded.map(maxDepth(_, query, refine)))
      //_ = println(s"Deepened: $deepened")
    } //yield deepened.map { case (term, count) => Facet(term, count) }.toList
    yield expanded.map { case (term, count) => Facet(term, count) }.toList

  private def partition(focus: IRI, query: CountFn, refine: MoreSpecificFn): Future[Map[MinimalTerm, Int]] = {
    //println(s"Get children of $focus")
    refine(focus).map { children =>
      Future.sequence(children.map { child =>
        //  println(s"Get count for child $child of $focus")
        query(child.iri).map(child -> _)
      }).map(_.toMap.filter { case (term, count) => count > 0 })
    }.flatten
  }

  private def expandMax(accPartitions: Map[MinimalTerm, Int], query: CountFn, refine: MoreSpecificFn): Future[Map[MinimalTerm, Int]] = {
    if (accPartitions.nonEmpty && accPartitions.size < minimumSize) {
      val (maxChild, maxChildCount) = accPartitions.maxBy(_._2)
      val subpartitionsFut = partition(maxChild.iri, query, refine)
      subpartitionsFut.flatMap { subpartitions =>
        if (subpartitions.size > 1) {
          val newPartitions = (accPartitions - maxChild) ++ subpartitions
          if (newPartitions.size < maximumSize) expandMax((accPartitions - maxChild) ++ subpartitions, query, refine)
          else if (newPartitions.size == maximumSize) Future.successful(newPartitions)
          else Future.successful(accPartitions)
        } else Future.successful(accPartitions)
      }
    } else Future.successful(accPartitions)
  }

  private def maxDepth(entry: (MinimalTerm, Int), query: CountFn, refine: MoreSpecificFn): Future[(MinimalTerm, Int)] = {
    // println(s"Deepening: $entry")
    val (focus, count) = entry
    partition(focus.iri, query, refine).flatMap { children =>
      if (children.size == 1 && children.head._2 == count && children.head._1 != focus) maxDepth(children.head, query, refine)
      else Future.successful(entry)
    }
  }

  final case class Facet(term: MinimalTerm, count: Int) extends JSONResultItem {

    def toJSON: JsObject = Map("term" -> term.toJSON, "count" -> count.toJson).toJson.asJsObject

  }

  object Facet {

    implicit val FacetResultsMarshaller: ToEntityMarshaller[Seq[Facet]] = Marshaller.combined(results =>
      new JsObject(Map("results" -> results.map(_.toJSON).toJson)))

    implicit val FacetMarshaller: ToEntityMarshaller[Facet] = Marshaller.combined(facet => facet.toJSON)

  }

}