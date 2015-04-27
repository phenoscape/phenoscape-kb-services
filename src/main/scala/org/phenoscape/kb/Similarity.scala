package org.phenoscape.kb

import org.phenoscape.owl.Vocab._
import org.phenoscape.owl.Vocab
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import scala.concurrent.Future
import spray.json._
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._
import org.phenoscape.kb.Main.system.dispatcher
import org.semanticweb.owlapi.model.OWLNamedIndividual
import org.semanticweb.owlapi.model.OWLClass
import scala.language.postfixOps
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import com.hp.hpl.jena.graph.Node_Variable
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery
import org.phenoscape.kb.Term.JSONResultItemsMarshaller

object Similarity {

  private val combined_score = ObjectProperty("http://purl.org/phenoscape/vocab.owl#combined_score")
  private val has_expect_score = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_expect_score")
  private val has_subsumer = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_subsumer")
  private val for_query_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_query_profile")
  private val for_corpus_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_corpus_profile")
  private val has_ic = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_ic")
  private val has_phenotypic_profile = ObjectProperty(Vocab.has_phenotypic_profile)
  private val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)

  def evolutionaryProfilesSimilarToGene(gene: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[SimilarityMatch]] =
    App.executeSPARQLQuery(geneToTaxonProfileQuery(gene, limit, offset), constructMatchFor(gene))

  def bestAnnotationsMatchesForComparison(gene: IRI, taxon: IRI): Future[Seq[AnnotationPair]] = {
    val geneInd = Individual(gene)
    val taxonInd = Individual(taxon)
    (for {
      results <- App.executeSPARQLQuery(comparisonSubsumersQuery(gene, taxon), Subsumer.fromQuery(_))
      subsumers <- Future.sequence(results)
      subsumersWithDisparity <- Future.sequence(subsumers.map(addDisparity))
    } yield {
      subsumersWithDisparity.filter(_.subsumer.ic > 0).sortBy(_.subsumer.ic).foldRight(Future(Seq.empty[UnlabelledAnnotationPair])) { (subsumerWithDisparity, pairsFuture) =>
        (for {
          pairs <- pairsFuture
        } yield {
          if (pairs.size < 20) {
            val geneAnnotationsFuture = subsumedAnnotationIRIs(geneInd, Class(subsumerWithDisparity.subsumer.term.iri))
            val taxonAnnotationsFuture = subsumedAnnotationIRIs(taxonInd, Class(subsumerWithDisparity.subsumer.term.iri))
            val newPairsFuture = for {
              pairs <- pairsFuture
              geneAnnotations <- geneAnnotationsFuture
              taxonAnnotations <- taxonAnnotationsFuture
            } yield {
              for {
                geneAnnotation <- geneAnnotations
                taxonAnnotation <- taxonAnnotations
                pair = UnlabelledAnnotationPair(geneAnnotation, taxonAnnotation, subsumerWithDisparity)
                if !pairs.exists(pair => pair.queryAnnotation == geneAnnotation && pair.corpusAnnotation == taxonAnnotation)
              } yield {
                pair
              }
            }
            newPairsFuture.map(pairs ++ _)
          } else {
            pairsFuture
          }
        }).flatMap(identity)
      }
    }).flatMap(identity).map(_.take(20)).flatMap(addLabels)
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

  def bestSubsumersForComparison(gene: IRI, taxon: IRI): Future[Seq[Subsumer]] = for {
    results <- App.executeSPARQLQuery(comparisonSubsumersQuery(gene, taxon), Subsumer.fromQuery(_))
    subsumers <- Future.sequence(results)
  } yield subsumers.filter(_.ic > 0)

  def subsumedAnnotationIRIs(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[IRI]] =
    App.executeSPARQLQuery(subsumedAnnotationsQuery(instance, subsumer), result => IRI.create(result.getResource("annotation").getURI))

  def subsumedAnnotations(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[MinimalTerm]] = for {
    irisFuture <- subsumedAnnotationIRIs(instance, subsumer)
    labelledTerms <- Future.sequence(irisFuture.map(Term.computedLabel(_)))
  } yield labelledTerms

  def profileSize(profileSubject: IRI): Future[Int] = {
    val query = select() where (
      bgp(
        t(profileSubject, has_phenotypic_profile / rdfType, 'annotation)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("annotation"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def icDisparity(term: OWLClass): Future[Double] = {
    val query = select_distinct('graph, 'ic) where (
      new ElementNamedGraph(new Node_Variable("graph"),
        bgp(
          t(term, has_ic, 'ic))))
    for {
      results <- App.executeSPARQLQuery(query, result => (result.getResource("graph").getURI, result.getLiteral("ic").getDouble))
    } yield {
      val values = results.toMap
      val differenceOpt = for {
        taxonIC <- values.get("http://kb.phenoscape.org/ic")
        geneIC <- values.get("http://kb.phenoscape.org/gene_ic")
      } yield {
        taxonIC - geneIC
      }
      differenceOpt.getOrElse(0.0)
    }
  }

  def addDisparity(subsumer: Subsumer): Future[SubsumerWithDisparity] =
    icDisparity(Class(subsumer.term.iri)).map(SubsumerWithDisparity(subsumer, _))

  //FIXME this query is way too slow
  def corpusSize: Future[Int] = {
    val query = select() where (
      bgp(
        t('comparison, for_corpus_profile, 'taxon_profile)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon_profile"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def geneToTaxonProfileQuery(gene: IRI, resultLimit: Int, resultOffset: Int): Query = {
    val query = select_distinct('taxon, 'taxon_label, 'median_score, 'expect_score) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/ic" where (
      bgp(
        t(gene, has_phenotypic_profile, 'gene_profile),
        t('comparison, for_query_profile, 'gene_profile),
        t('comparison, combined_score, 'median_score),
        t('comparison, has_expect_score, 'expect_score),
        t('comparison, for_corpus_profile, 'taxon_profile),
        t('taxon, has_phenotypic_profile, 'taxon_profile),
        t('taxon, rdfsLabel, 'taxon_label))) order_by (asc('expect_score), asc('median_score), asc('taxon))
    if (resultLimit > 1) {
      query.setLimit(resultLimit)
      query.setOffset(resultOffset)
    }
    query
  }

  def comparisonSubsumersQuery(gene: IRI, taxon: IRI): Query =
    select_distinct('subsumer, 'ic) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/ic" where (
      bgp(
        t(gene, has_phenotypic_profile, 'gene_profile),
        t(taxon, has_phenotypic_profile, 'taxon_profile),
        t('comparison, for_query_profile, 'gene_profile),
        t('comparison, for_corpus_profile, 'taxon_profile),
        t('comparison, has_subsumer, 'subsumer),
        t('subsumer, has_ic, 'ic)))

  def subsumedAnnotationsQuery(instance: OWLNamedIndividual, subsumer: OWLClass): Query =
    select_distinct('annotation) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/ic" where (
      bgp(
        t(instance, has_phenotypic_profile / rdfType, 'annotation)),
        new ElementSubQuery(select('annotation) where (
          bgp(
            t('annotation, rdfsSubClassOf*, subsumer)))))

  def constructMatchFor(gene: IRI): QuerySolution => SimilarityMatch =
    (result: QuerySolution) => SimilarityMatch(
      MinimalTerm(IRI.create(result.getResource("taxon").getURI), result.getLiteral("taxon_label").getLexicalForm),
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
    val header = "taxon IRI\ttaxon label\tmedian score\texpect score"
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

case class UnlabelledAnnotationPair(queryAnnotation: IRI, corpusAnnotation: IRI, bestSubsumer: SubsumerWithDisparity)

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