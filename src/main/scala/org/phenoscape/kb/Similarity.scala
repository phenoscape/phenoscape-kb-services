package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.graph.{NodeFactory, Node_Variable}
import org.apache.jena.query.{Query, QuerySolution}
import org.apache.jena.sparql.path.Path
import org.apache.jena.sparql.syntax._
import org.phenoscape.kb.Graph.getTermSubsumerPairs
import org.phenoscape.kb.JSONResultItem.JSONResultItemsMarshaller
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.util.PropertyPathParser
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.phenoscape.sparql.FromQuerySolutionOWL._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.semanticweb.owlapi.model.{IRI, OWLClass, OWLNamedIndividual}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.Future
import scala.language.postfixOps

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

  def querySimilarProfiles(queryItem: IRI,
                           corpus: IRI,
                           limit: Int = 20,
                           offset: Int = 0): Future[Seq[SimilarityMatch]] =
    App.executeSPARQLQuery(similarityProfileQuery(queryItem, corpus, limit, offset), constructMatchFor(queryItem))

  def bestAnnotationsMatchesForComparison(queryItem: IRI,
                                          queryGraph: IRI,
                                          corpusItem: IRI,
                                          corpusGraph: IRI): Future[Seq[UnlabelledAnnotationPair]] = {
    val queryInd = Individual(queryItem)
    val corpusInd = Individual(corpusItem)
    (for {
      subsumers <- bestSubsumersForComparison(queryItem, corpusItem, corpusGraph)
      subsumersWithDisparity <- Future.sequence(subsumers.map(addDisparity(_, queryGraph, corpusGraph)))
    } yield subsumersWithDisparity.sortBy(_.subsumer.ic).foldRight(Future(Seq.empty[UnlabelledAnnotationPair])) {
      (subsumerWithDisparity, pairsFuture) =>
        (for {
          pairs <- pairsFuture
        } yield
          if (pairs.size < 20) {
            val queryAnnotationsFuture =
              subsumedAnnotationIRIs(queryInd, Class(subsumerWithDisparity.subsumer.term.iri))
            val corpusAnnotationsFuture =
              subsumedAnnotationIRIs(corpusInd, Class(subsumerWithDisparity.subsumer.term.iri))
            val newPairsFuture = for {
              pairs <- pairsFuture
              queryAnnotations <- queryAnnotationsFuture
              corpusAnnotations <- corpusAnnotationsFuture
            } yield for {
              queryAnnotation <- queryAnnotations
              if !pairs.exists(pair => pair.queryAnnotation == queryAnnotation)
              corpusAnnotation <- corpusAnnotations.headOption
            } yield UnlabelledAnnotationPair(queryAnnotation, corpusAnnotation, subsumerWithDisparity)
            newPairsFuture.map(pairs ++ _)
          } else
            pairsFuture).flatMap(identity)
    }).flatMap(identity).map(_.take(20))
  }

  def addLabels(unlabelleds: Seq[UnlabelledAnnotationPair]): Future[Seq[AnnotationPair]] = {
    val labelled = unlabelleds.foldLeft(Future(Map.empty[IRI, String], Seq.empty[AnnotationPair])) {
      (accFuture, pair) =>
        (for {
          (labelMap, pairs) <- accFuture
        } yield {
          val queryAnnotationLabelFuture = labelMap
            .get(pair.queryAnnotation)
            .map(Future.successful)
            .getOrElse(Term.computedLabel(pair.queryAnnotation).map(_.label.getOrElse(pair.queryAnnotation.toString)))
          val corpusAnnotationLabelFuture = labelMap
            .get(pair.corpusAnnotation)
            .map(Future.successful)
            .getOrElse(Term.computedLabel(pair.corpusAnnotation).map(_.label.getOrElse(pair.corpusAnnotation.toString)))
          for {
            queryAnnotationLabel <- queryAnnotationLabelFuture
            corpusAnnotationLabel <- corpusAnnotationLabelFuture
          } yield (labelMap + (pair.queryAnnotation -> queryAnnotationLabel) + (pair.corpusAnnotation -> corpusAnnotationLabel),
                   pairs :+ AnnotationPair(MinimalTerm(pair.queryAnnotation, Some(queryAnnotationLabel)),
                                           MinimalTerm(pair.corpusAnnotation, Some(corpusAnnotationLabel)),
                                           pair.bestSubsumer))
        }).flatMap(identity)
    }
    labelled.map(_._2)
  }

  def bestSubsumersForComparison(queryItem: IRI, corpusItem: IRI, corpusGraph: IRI): Future[Seq[Subsumer]] =
    for {
      results <-
        App.executeSPARQLQuery(comparisonSubsumersQuery(queryItem, corpusItem, corpusGraph), Subsumer.fromQuery(_))
      subsumers <- Future.sequence(results)
    } yield subsumers.filter(_.ic > 0)

  def subsumedAnnotationIRIs(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[IRI]] =
    App.executeSPARQLQuery(subsumedAnnotationsQuery(instance, subsumer),
                           result => IRI.create(result.getResource("annotation").getURI))

  def subsumedAnnotations(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[MinimalTerm]] =
    for {
      irisFuture <- subsumedAnnotationIRIs(instance, subsumer)
      labelledTerms <- Future.sequence(irisFuture.map(Term.computedLabel))
    } yield labelledTerms

  def getProfile(profileSubject: IRI, corpus: PhenotypeCorpus): Future[Seq[IRI]] = {
    final case class Annotation(annotation: IRI)
    val query =
      sparql"""
               SELECT DISTINCT ?annotation
               FROM $KBMainGraph
               FROM $KBClosureGraph
               WHERE {
                $profileSubject ${corpus.path} ?annotation .
                FILTER(isIRI(?annotation))
                }
              """
    App.executeSPARQLQueryStringCase[Annotation](query.text).map(_.map(_.annotation))
  }

  def profileSize(profileSubject: IRI, corpus: PhenotypeCorpus): Future[Int] = {
    val query =
      sparql"""
               SELECT (COUNT(DISTINCT ?phen) AS ?count) 
               FROM $KBMainGraph
               FROM $KBClosureGraph
               WHERE {
                $profileSubject ${corpus.path} ?phen .
                FILTER(isIRI(?phen))
                }
              """
    App.executeSPARQLQuery(query.toQuery).map(ResultCount.count)
  }

  def icDisparity(term: OWLClass, queryGraph: IRI, corpusGraph: IRI): Future[Double] = {
    val query =
      select_distinct('graph, 'ic) where (new ElementNamedGraph(new Node_Variable("graph"), bgp(t(term, has_ic, 'ic))))
    for {
      results <-
        App.executeSPARQLQuery(query, result => (result.getResource("graph").getURI, result.getLiteral("ic").getDouble))
    } yield {
      val values = results.toMap
      val differenceOpt = for {
        corpusIC <- values.get(corpusGraph.toString)
        queryIC <- values.get(queryGraph.toString)
      } yield corpusIC - queryIC
      differenceOpt.getOrElse(0.0)
    }
  }

  def addDisparity(subsumer: Subsumer, queryGraph: IRI, corpusGraph: IRI): Future[SubsumerWithDisparity] =
    icDisparity(Class(subsumer.term.iri), queryGraph, corpusGraph).map(SubsumerWithDisparity(subsumer, _))

  def corpusSize(corpus: PhenotypeCorpus): Future[Int] = {
    val query =
      sparql"""
            SELECT (COUNT(DISTINCT ?item) AS ?count)
            WHERE {
              ?item ${corpus.path} ?term .
              ${corpus.constraint(sparql"?item")}
              FILTER(isIRI(?item))
              FILTER(isIRI(?term))
            }
            """
    App.executeSPARQLQuery(query.toQuery).map(ResultCount.count)
  }

  private def triplesBlock(elements: Element*): ElementGroup = {
    val block = new ElementGroup()
    elements.foreach(block.addElement)
    block
  }

  def similarityProfileQuery(queryItem: IRI, corpusGraph: IRI, resultLimit: Int, resultOffset: Int): Query = {
    val query = select_distinct('corpus_item,
                                'corpus_item_label,
                                'median_score,
                                'expect_score) from "http://kb.phenoscape.org/" from corpusGraph.toString where (
      bgp(
        t(queryItem, has_phenotypic_profile, 'query_profile),
        t('comparison, for_query_profile, 'query_profile),
        t('comparison, combined_score, 'median_score),
        t('comparison, has_expect_score, 'expect_score),
        t('comparison, for_corpus_profile, 'corpus_profile),
        t('corpus_item, has_phenotypic_profile, 'corpus_profile),
        t('corpus_item, Vocab.rdfsLabel, 'corpus_item_label)
      )
    ) order_by (asc('expect_score), asc('median_score), asc('corpus_item_label))
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
        t('subsumer, has_ic, 'ic)
      )
    )

  def subsumedAnnotationsQuery(instance: OWLNamedIndividual, subsumer: OWLClass): Query =
    select_distinct('annotation) from "http://kb.phenoscape.org/" where (bgp(
      t(instance, has_phenotypic_profile / rdfType, 'annotation)),
    new ElementSubQuery(
      select('annotation) where (new ElementNamedGraph(NodeFactory.createURI("http://kb.phenoscape.org/closure"),
                                                       bgp(t('annotation, rdfsSubClassOf, subsumer))))))

  def constructMatchFor(queryItem: IRI): QuerySolution => SimilarityMatch =
    (result: QuerySolution) =>
      SimilarityMatch(
        MinimalTerm(IRI.create(result.getResource("corpus_item").getURI),
                    Some(result.getLiteral("corpus_item_label").getLexicalForm)),
        result.getLiteral("median_score").getDouble,
        result.getLiteral("expect_score").getDouble
      )

  def stateSimilarity(leftStudyIRI: IRI,
                      leftCharacterNum: Int,
                      leftSymbol: String,
                      rightStudyIRI: IRI,
                      rightCharacterNum: Int,
                      rightSymbol: String): Future[Double] = {
    val leftSubsumersFut = stateSubsumers(leftStudyIRI, leftCharacterNum, leftSymbol)
    val rightSubsumersFut = stateSubsumers(rightStudyIRI, rightCharacterNum, rightSymbol)
    for {
      leftSubsumers <- leftSubsumersFut
      rightSubsumers <- rightSubsumersFut
    } yield {
      val intersectionCount = leftSubsumers.intersect(rightSubsumers).size
      val unionCount = (leftSubsumers ++ rightSubsumers).size
      intersectionCount.toDouble / unionCount.toDouble
    }
  }

  def pairwiseJaccardSimilarity(iris: Set[IRI], corpusOpt: Option[PhenotypeCorpus]): Future[Seq[JaccardScore]] = {
    val termSubsumersMapFut = for {
      termSubsumerPairs <- getTermSubsumerPairs(iris, corpusOpt)
    } yield {
      val groupedByTerm = termSubsumerPairs.groupBy(_._1)
      groupedByTerm
        .map { case (term, pairs) =>
          val subsumers = pairs.map(_._2).toSet
          (term, subsumers)
        }
    }
    termSubsumersMapFut.map { termSubsumersMap =>
      (for {
        combo <- iris.toSeq.combinations(2)
        left = combo(0)
        right = combo(1)
        intersectionCount = termSubsumersMap
          .getOrElse(left, Set.empty)
          .intersect(termSubsumersMap.getOrElse(right, Set.empty))
          .size
        unionCount = (termSubsumersMap.getOrElse(left, Set.empty) ++ termSubsumersMap.getOrElse(right, Set.empty)).size
        jaccardScore = if (unionCount == 0) 0 else intersectionCount.toDouble / unionCount.toDouble
      } yield JaccardScore(Set(left, right), jaccardScore)).toSeq
    }
  }

  private def stateSubsumers(studyIRI: IRI, characterNum: Int, symbol: String): Future[Set[IRI]] = {
    val query: QueryText =
      sparql"""
              PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
              SELECT DISTINCT ?subsumer
              FROM $KBMainGraph
              FROM $KBClosureGraph
              WHERE {
                $studyIRI $has_character ?character .
                ?character $list_index $characterNum .
                ?character $may_have_state_value ?state .
                ?state $state_symbol $symbol^^xsd:string .
                ?state $describes_phenotype ?phenotype .
                ?phenotype $rdfsSubClassOf ?subsumer .
              }
            """
    App.executeSPARQLQueryString(query.text, qs => IRI.create(qs.getResource("subsumer").getURI)).map(_.toSet)
  }

  sealed trait PhenotypeCorpus {

    def path: Path

    def constraint(node: QueryText): QueryText

  }

  object StateCorpus extends PhenotypeCorpus {

    val path: Path = PropertyPathParser
      .parsePropertyPath(sparql"$describes_phenotype".text)
      .getOrElse(throw new Exception("Invalid property path"))

    def constraint(node: QueryText): QueryText = sparql""

  }

  object TaxonCorpus extends PhenotypeCorpus {

    val path: Path = PropertyPathParser
      .parsePropertyPath(sparql"$has_phenotypic_profile/$rdfType".text)
      .getOrElse(throw new Exception("Invalid property path"))

    def constraint(node: QueryText): QueryText = sparql"$node $rdfsIsDefinedBy $VTO ."

  }

  sealed trait SimilarityTermType {

    def path: Path

  }

  object AnatomyTerm extends SimilarityTermType {

    private val phenotypeOfSome = ObjectProperty(s"${phenotype_of.getIRI.toString}_some")

    val path: Path = PropertyPathParser
      .parsePropertyPath(sparql"$rdfsSubClassOf/$phenotypeOfSome".text)
      .getOrElse(throw new Exception("Invalid property path"))

  }

  object PhenotypeTerm extends SimilarityTermType {

    val path: Path = PropertyPathParser
      .parsePropertyPath(sparql"$rdfsSubClassOf".text)
      .getOrElse(throw new Exception("Invalid property path"))

  }

  def frequency(terms: Set[IRI], corpus: PhenotypeCorpus, termType: SimilarityTermType): Future[TermFrequencyTable] = {
    val termsValues = terms.map(t => sparql" $t ").reduceOption(_ + _).getOrElse(sparql"")
    val query =
      sparql"""
        SELECT ?term (COUNT(DISTINCT ?item) AS ?count)
        WHERE {
          VALUES ?term { $termsValues }
          ?cls ${termType.path} ?term .
          ?item ${corpus.path} ?cls .
          ${corpus.constraint(sparql"?item")}
        }
        GROUP BY ?term
        """
    for {
      results <- App.executeSPARQLQueryString(
        query.text,
        qs => IRI.create(qs.getResource("term").getURI) -> qs.getLiteral("count").getInt,
        App.QLeverEndpoint
      )
      resultsMap = results.toMap
      foundTerms = resultsMap.keySet
      // add in 0 counts for terms not returned by the query
    } yield resultsMap ++ (terms -- foundTerms).map(_ -> 0)
  }

  type TermFrequencyTable = Map[IRI, Int]

  object TermFrequencyTable {

    implicit val TermFrequencyTableCSV: ToEntityMarshaller[TermFrequencyTable] =
      Marshaller.stringMarshaller(MediaTypes.`text/csv`).compose { table =>
        table.keys.toSeq.sortBy(_.toString).map(k => s"$k,${table(k)}").mkString("\n")
      }

  }

}

case class SimilarityMatch(corpusProfile: MinimalTerm, medianScore: Double, expectScore: Double)
    extends JSONResultItem {

  def toJSON: JsObject =
    Map("match_profile" -> corpusProfile.toJSON,
        "median_score" -> medianScore.toJson,
        "expect_score" -> expectScore.toJson).toJson.asJsObject

  override def toString(): String =
    s"${corpusProfile.iri.toString}\t${corpusProfile.label}\t$medianScore\t$expectScore"

}

object SimilarityMatch {

  implicit val SimilarityMatchMarshaller: ToEntityMarshaller[SimilarityMatch] =
    Marshaller.stringMarshaller(MediaTypes.`text/plain`).compose(_.toString)

  implicit val SimilarityMatchesTextMarshaller: ToEntityMarshaller[Seq[SimilarityMatch]] =
    Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { matches =>
      val header = "match IRI\tmatch label\tmedian score\texpect score"
      s"$header\n${matches.map(_.toString).mkString("\n")}"
    }

  implicit val ComboSimilarityMatchesMarshaller =
    Marshaller.oneOf(SimilarityMatchesTextMarshaller, JSONResultItemsMarshaller)

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
    extends JSONResultItem {

  def toJSON: JsObject =
    Map(
      "query_annotation" -> Map("@id" -> queryAnnotation.toString).toJson,
      "corpus_annotation" -> Map("@id" -> corpusAnnotation.toString).toJson,
      "best_subsumer" -> bestSubsumer.toJSON
    ).toJson.asJsObject

}

case class AnnotationPair(queryAnnotation: MinimalTerm,
                          corpusAnnotation: MinimalTerm,
                          bestSubsumer: SubsumerWithDisparity)
    extends JSONResultItem {

  def toJSON: JsObject =
    Map("query_annotation" -> queryAnnotation.toJSON,
        "corpus_annotation" -> corpusAnnotation.toJSON,
        "best_subsumer" -> bestSubsumer.toJSON).toJson.asJsObject

}

case class Subsumer(term: MinimalTerm, ic: Double) extends JSONResultItem {

  def toJSON: JsObject =
    Map("term" -> term.toJSON, "ic" -> ic.toJson).toJson.asJsObject

}

case class SubsumerWithDisparity(subsumer: Subsumer, disparity: Double) extends JSONResultItem {

  def toJSON: JsObject =
    Map("term" -> subsumer.term.toJSON, "ic" -> subsumer.ic.toJson, "disparity" -> disparity.toJson).toJson.asJsObject

}

object Subsumer {

  def fromQuery(result: QuerySolution): Future[Subsumer] = {
    val iri = IRI.create(result.getResource("subsumer").getURI)
    Term.computedLabel(iri).map(term => Subsumer(term, result.getLiteral("ic").getDouble))
  }

}

case class JaccardScore(terms: Set[IRI], score: Double) extends JSONResultItem {

  def toJSON: JsObject =
    Map("terms" -> terms.map(_.toString).toJson, "score" -> score.toJson).toJson.asJsObject()

}
