package org.phenoscape.kb

import org.phenoscape.owl.Vocab._
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

object Similarity {

  private val combined_score = ObjectProperty("http://purl.org/phenoscape/vocab.owl#combined_score")
  private val has_subsumer = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_subsumer")
  private val for_query_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_query_profile")
  private val for_corpus_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_corpus_profile")
  private val corpus_frequency = ObjectProperty("http://purl.org/phenoscape/vocab.owl#corpus_frequency")

  def evolutionaryProfilesSimilarToGene(gene: IRI): Future[Seq[SimilarityMatch]] =
    App.executeSPARQLQuery(geneToTaxonProfileQuery(gene), constructMatchFor(gene))

  def bestSubsumersForComparison(gene: IRI, taxon: IRI): Future[Seq[Subsumer]] =
    App.executeSPARQLQuery(comparisonSubsumersQuery(gene, taxon), Subsumer(_))

  def geneToTaxonProfileQuery(gene: IRI): Query =
    select_distinct('taxon, 'score) where (
      bgp(
        t(gene, has_phenotypic_profile, 'gene_profile),
        t('comparison, for_query_profile, 'gene_profile),
        t('comparison, combined_score, 'score),
        t('comparison, for_corpus_profile, 'taxon_profile),
        t('taxon, has_phenotypic_profile, 'taxon_profile))) order_by desc('score) limit 20

  def comparisonSubsumersQuery(gene: IRI, taxon: IRI): Query =
    select_distinct('subsumer, 'frequency) where (
      bgp(
        t(gene, has_phenotypic_profile, 'gene_profile),
        t(taxon, has_phenotypic_profile, 'taxon_profile),
        t('comparison, for_query_profile, 'gene_profile),
        t('comparison, for_query_profile, 'taxon_profile),
        t('comparison, has_subsumer, 'subsumer),
        t('subsumer, corpus_frequency, 'frequency)))

  def constructMatchFor(gene: IRI): QuerySolution => SimilarityMatch =
    (result: QuerySolution) => SimilarityMatch(gene,
      IRI.create(result.getResource("taxon").getURI),
      result.getLiteral("score").getDouble)

}

case class SimilarityMatch(geneProfile: IRI, corpusProfile: IRI, score: Double) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("query_profile" -> geneProfile.toString.toJson,
      "match_profile" -> corpusProfile.toString.toJson,
      "score" -> score.toJson).toJson.asJsObject
  }

}

case class Subsumer(iri: IRI, corpusFrequency: Int) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString.toJson,
      "corpus_frequency" -> corpusFrequency.toJson).toJson.asJsObject
  }

}

object Subsumer {

  def apply(result: QuerySolution): Subsumer = Subsumer(
    IRI.create(result.getResource("subsumer").getURI),
    result.getLiteral("score").getInt)

}