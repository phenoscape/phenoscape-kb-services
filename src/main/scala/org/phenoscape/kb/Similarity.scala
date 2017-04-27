package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.Node_Variable
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_NotOneOf
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.syntax.Element
import org.apache.jena.sparql.syntax.ElementFilter
import org.apache.jena.sparql.syntax.ElementGroup
import org.apache.jena.sparql.syntax.ElementNamedGraph
import org.apache.jena.sparql.syntax.ElementSubQuery
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.model.OWLNamedIndividual

import spray.http._
import spray.httpx._
import spray.httpx.marshalling._
import spray.json._
import spray.json.DefaultJsonProtocol._

object Similarity {

  private val combined_score = ObjectProperty("http://purl.org/phenoscape/vocab.owl#combined_score")
  private val has_expect_score = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_expect_score")
  private val has_subsumer = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_subsumer")
  private val for_query_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_query_profile")
  private val for_corpus_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_corpus_profile")
  private val has_ic = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_ic")
  val TaxaCorpus = IRI.create("http://kb.phenoscape.org/sim/taxa")
  val GenesCorpus = IRI.create("http://kb.phenoscape.org/sim/genes")
  private val has_phenotypic_profile = ObjectProperty(Vocab.has_phenotypic_profile)
  private val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)

  val availableCorpora = Seq(TaxaCorpus, GenesCorpus)

  def evolutionaryProfilesSimilarToGene(gene: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[SimilarityMatch]] =
    App.executeSPARQLQuery(similarityProfileQuery(gene, TaxaCorpus, limit, offset), constructMatchFor(gene))

  def querySimilarProfiles(queryItem: IRI, corpus: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[SimilarityMatch]] =
    App.executeSPARQLQuery(similarityProfileQuery(queryItem, corpus, limit, offset), constructMatchFor(queryItem))

  def bestAnnotationsMatchesForComparison(queryItem: IRI, queryGraph: IRI, corpusItem: IRI, corpusGraph: IRI): Future[Seq[UnlabelledAnnotationPair]] = {
    val queryInd = Individual(queryItem)
    val corpusInd = Individual(corpusItem)
    (for {
      subsumers <- bestSubsumersForComparison(queryItem, corpusItem, corpusGraph)
      subsumersWithDisparity <- Future.sequence(subsumers.map(addDisparity(_, queryGraph, corpusGraph)))
    } yield {
      subsumersWithDisparity.sortBy(_.subsumer.ic).foldRight(Future(Seq.empty[UnlabelledAnnotationPair])) { (subsumerWithDisparity, pairsFuture) =>
        (for {
          pairs <- pairsFuture
        } yield {
          if (pairs.size < 20) {
            val queryAnnotationsFuture = subsumedAnnotationIRIs(queryInd, Class(subsumerWithDisparity.subsumer.term.iri))
            val corpusAnnotationsFuture = subsumedAnnotationIRIs(corpusInd, Class(subsumerWithDisparity.subsumer.term.iri))
            val newPairsFuture = for {
              pairs <- pairsFuture
              queryAnnotations <- queryAnnotationsFuture
              corpusAnnotations <- corpusAnnotationsFuture
            } yield {
              for {
                queryAnnotation <- queryAnnotations
                if !pairs.exists(pair => pair.queryAnnotation == queryAnnotation)
                corpusAnnotation <- corpusAnnotations.headOption
              } yield {
                UnlabelledAnnotationPair(queryAnnotation, corpusAnnotation, subsumerWithDisparity)
              }
            }
            newPairsFuture.map(pairs ++ _)
          } else {
            pairsFuture
          }
        }).flatMap(identity)
      }
    }).flatMap(identity).map(_.take(20))
  }

  def addLabels(unlabelleds: Seq[UnlabelledAnnotationPair]): Future[Seq[AnnotationPair]] = {
    val labelled = unlabelleds.foldLeft(Future(Map.empty[IRI, String], Seq.empty[AnnotationPair])) { (accFuture, pair) =>
      (for {
        (labelMap, pairs) <- accFuture
      } yield {
        val queryAnnotationLabelFuture = labelMap.get(pair.queryAnnotation).map(Future.successful).getOrElse(Term.computedLabel(pair.queryAnnotation).map(_.label))
        val corpusAnnotationLabelFuture = labelMap.get(pair.corpusAnnotation).map(Future.successful).getOrElse(Term.computedLabel(pair.corpusAnnotation).map(_.label))
        for {
          queryAnnotationLabel <- queryAnnotationLabelFuture
          corpusAnnotationLabel <- corpusAnnotationLabelFuture
        } yield {
          (labelMap + (pair.queryAnnotation -> queryAnnotationLabel) + (pair.corpusAnnotation -> corpusAnnotationLabel),
            pairs :+ AnnotationPair(
              MinimalTerm(pair.queryAnnotation, queryAnnotationLabel),
              MinimalTerm(pair.corpusAnnotation, corpusAnnotationLabel),
              pair.bestSubsumer))
        }
      }).flatMap(identity)
    }
    labelled.map(_._2)
  }

  def bestSubsumersForComparison(queryItem: IRI, corpusItem: IRI, corpusGraph: IRI): Future[Seq[Subsumer]] = for {
    results <- App.executeSPARQLQuery(comparisonSubsumersQuery(queryItem, corpusItem, corpusGraph), Subsumer.fromQuery(_))
    subsumers <- Future.sequence(results)
  } yield subsumers.filter(_.ic > 0)

  def subsumedAnnotationIRIs(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[IRI]] =
    App.executeSPARQLQuery(subsumedAnnotationsQuery(instance, subsumer), result => IRI.create(result.getResource("annotation").getURI))

  def subsumedAnnotations(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[MinimalTerm]] = for {
    irisFuture <- subsumedAnnotationIRIs(instance, subsumer)
    labelledTerms <- Future.sequence(irisFuture.map(Term.computedLabel(_)))
  } yield labelledTerms

  def profileSize(profileSubject: IRI): Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t(profileSubject, has_phenotypic_profile / rdfType, 'annotation)),
        new ElementFilter(new E_NotOneOf(new ExprVar('annotation), new ExprList(List(
          new NodeValueNode(AnnotatedPhenotype),
          new NodeValueNode(owlNamedIndividual))))))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("annotation"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def icDisparity(term: OWLClass, queryGraph: IRI, corpusGraph: IRI): Future[Double] = {
    val query = select_distinct('graph, 'ic) where (
      new ElementNamedGraph(new Node_Variable("graph"),
        bgp(
          t(term, has_ic, 'ic))))
    for {
      results <- App.executeSPARQLQuery(query, result => (result.getResource("graph").getURI, result.getLiteral("ic").getDouble))
    } yield {
      val values = results.toMap
      val differenceOpt = for {
        corpusIC <- values.get(corpusGraph.toString)
        queryIC <- values.get(queryGraph.toString)
      } yield {
        corpusIC - queryIC
      }
      differenceOpt.getOrElse(0.0)
    }
  }

  def addDisparity(subsumer: Subsumer, queryGraph: IRI, corpusGraph: IRI): Future[SubsumerWithDisparity] =
    icDisparity(Class(subsumer.term.iri), queryGraph, corpusGraph).map(SubsumerWithDisparity(subsumer, _))

  //FIXME this query is way too slow
  def corpusSize(corpusGraph: IRI): Future[Int] = {
    val query = select() from corpusGraph.toString where (
      bgp(
        t('comparison, for_corpus_profile, 'profile)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("profile"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  private def triplesBlock(elements: Element*): ElementGroup = {
    val block = new ElementGroup()
    elements.foreach(block.addElement)
    block
  }

  def similarityProfileQuery(queryItem: IRI, corpusGraph: IRI, resultLimit: Int, resultOffset: Int): Query = {
    val query = select_distinct('corpus_item, 'corpus_item_label, 'median_score, 'expect_score) from "http://kb.phenoscape.org/" from corpusGraph.toString where (
      bgp(
        t(queryItem, has_phenotypic_profile, 'query_profile),
        t('comparison, for_query_profile, 'query_profile),
        t('comparison, combined_score, 'median_score),
        t('comparison, has_expect_score, 'expect_score),
        t('comparison, for_corpus_profile, 'corpus_profile),
        t('corpus_item, has_phenotypic_profile, 'corpus_profile),
        t('corpus_item, Vocab.rdfsLabel, 'corpus_item_label))) order_by (asc('expect_score), asc('median_score), asc('corpus_item_label))
    if (resultLimit > 1) {
      query.setLimit(resultLimit)
      query.setOffset(resultOffset)
    }
    query
  }

  def comparisonSubsumersQuery(queryItem: IRI, corpusItem: IRI, corpusGraph: IRI): Query =
    select_distinct('subsumer, 'ic) from "http://kb.phenoscape.org/" from corpusGraph.toString where (
      bgp(
        t(queryItem, has_phenotypic_profile, 'query_profile),
        t(corpusItem, has_phenotypic_profile, 'corpus_profile),
        t('comparison, for_query_profile, 'query_profile),
        t('comparison, for_corpus_profile, 'corpus_profile),
        t('comparison, has_subsumer, 'subsumer),
        t('subsumer, has_ic, 'ic)))

  def subsumedAnnotationsQuery(instance: OWLNamedIndividual, subsumer: OWLClass): Query =
    select_distinct('annotation) from "http://kb.phenoscape.org/" where (
      bgp(
        t(instance, has_phenotypic_profile / rdfType, 'annotation)),
        new ElementSubQuery(select('annotation) where (
          new ElementNamedGraph(NodeFactory.createURI("http://kb.phenoscape.org/closure"),
            bgp(
              t('annotation, rdfsSubClassOf, subsumer))))))

  def constructMatchFor(queryItem: IRI): QuerySolution => SimilarityMatch =
    (result: QuerySolution) => SimilarityMatch(
      MinimalTerm(IRI.create(result.getResource("corpus_item").getURI), result.getLiteral("corpus_item_label").getLexicalForm),
      result.getLiteral("median_score").getDouble,
      result.getLiteral("expect_score").getDouble)

}

case class SimilarityMatch(corpusProfile: MinimalTerm, medianScore: Double, expectScore: Double) extends JSONResultItem {

  def toJSON: JsObject = {
    Map(
      "match_profile" -> corpusProfile.toJSON,
      "median_score" -> medianScore.toJson,
      "expect_score" -> expectScore.toJson).toJson.asJsObject
  }

  override def toString(): String = {
    s"${corpusProfile.iri.toString}\t${corpusProfile.label}\t$medianScore\t$expectScore"
  }

}

object SimilarityMatch {

  implicit val SimilarityMatchMarshaller = Marshaller.delegate[SimilarityMatch, String](MediaTypes.`text/plain`)(_.toString)

  val SimilarityMatchesTextMarshaller = Marshaller.delegate[Seq[SimilarityMatch], String](MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`) { matches =>
    val header = "match IRI\tmatch label\tmedian score\texpect score"
    s"$header\n${matches.map(_.toString).mkString("\n")}"
  }

  implicit val ComboSimilarityMatchesMarshaller = ToResponseMarshaller.oneOf(MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`, MediaTypes.`application/json`)(SimilarityMatchesTextMarshaller, JSONResultItemsMarshaller)

}

//case class SimilarityMatches(matches: Seq[SimilarityMatch])
//
//object SimilarityMatches {
//
//  val SimilarityMatchesMarshaller = Marshaller.delegate[SimilarityMatches, String](MediaTypes.`text/tab-separated-values`) { matches =>
//    val header = "taxon IRI\ttaxon label\tmedian score\texpect score"
//    s"$header\n${matches.matches.map(_.toString).mkString("\n")}"
//  }
//
//  val SimilarityMatchesJSONMarshaller = Marshaller.delegate[SimilarityMatches, Seq[SimilarityMatch]](MediaTypes.`application/json`) { matches =>
//    matches.matches
//  }
//
//  implicit val ComboSimilarityMatchesMarshaller = ToResponseMarshaller.oneOf(MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`, MediaTypes.`application/json`)(SimilarityMatchesMarshaller, SimilarityMatchesJSONMarshaller)
//
//}

case class UnlabelledAnnotationPair(queryAnnotation: IRI, corpusAnnotation: IRI, bestSubsumer: SubsumerWithDisparity) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("query_annotation" -> Map("@id" -> queryAnnotation.toString).toJson,
      "corpus_annotation" -> Map("@id" -> corpusAnnotation.toString).toJson,
      "best_subsumer" -> bestSubsumer.toJSON).toJson.asJsObject
  }

}

case class AnnotationPair(queryAnnotation: MinimalTerm, corpusAnnotation: MinimalTerm, bestSubsumer: SubsumerWithDisparity) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("query_annotation" -> queryAnnotation.toJSON,
      "corpus_annotation" -> corpusAnnotation.toJSON,
      "best_subsumer" -> bestSubsumer.toJSON).toJson.asJsObject
  }

}

case class Subsumer(term: MinimalTerm, ic: Double) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("term" -> term.toJSON,
      "ic" -> ic.toJson).toJson.asJsObject
  }

}

case class SubsumerWithDisparity(subsumer: Subsumer, disparity: Double) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("term" -> subsumer.term.toJSON,
      "ic" -> subsumer.ic.toJson,
      "disparity" -> disparity.toJson).toJson.asJsObject
  }

}

object Subsumer {

  def fromQuery(result: QuerySolution): Future[Subsumer] = {
    val iri = IRI.create(result.getResource("subsumer").getURI)
    Term.computedLabel(iri).map(term => Subsumer(term, result.getLiteral("ic").getDouble))
  }

}