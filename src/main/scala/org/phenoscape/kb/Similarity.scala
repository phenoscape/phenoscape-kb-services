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

object Similarity {

  private val combined_score = ObjectProperty("http://purl.org/phenoscape/vocab.owl#combined_score")
  private val has_subsumer = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_subsumer")
  private val for_query_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_query_profile")
  private val for_corpus_profile = ObjectProperty("http://purl.org/phenoscape/vocab.owl#for_corpus_profile")
  private val has_ic = ObjectProperty("http://purl.org/phenoscape/vocab.owl#has_ic")
  private val has_phenotypic_profile = ObjectProperty(Vocab.has_phenotypic_profile)
  private val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)

  def evolutionaryProfilesSimilarToGene(gene: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[SimilarityMatch]] =
    App.executeSPARQLQuery(geneToTaxonProfileQuery(gene, limit, offset), constructMatchFor(gene))

  def bestSubsumersForComparison(gene: IRI, taxon: IRI): Future[Seq[Subsumer]] = for {
    results <- App.executeSPARQLQuery(comparisonSubsumersQuery(gene, taxon), Subsumer.fromQuery(_))
    subsumers <- Future.sequence(results)
  } yield subsumers

  def subsumedAnnotations(instance: OWLNamedIndividual, subsumer: OWLClass): Future[Seq[MinimalTerm]] = for {
    results <- App.executeSPARQLQuery(subsumedAnnotationsQuery(instance, subsumer), result => Term.computedLabel(IRI.create(result.getResource("annotation").getURI)))
    annotations <- Future.sequence(results)
  } yield annotations

  def profileSize(profileSubject: IRI): Future[Int] = {
    val query = select() where (
      bgp(
        t(profileSubject, has_phenotypic_profile / rdfType, 'annotation)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("annotation"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def geneToTaxonProfileQuery(gene: IRI, resultLimit: Int, resultOffset: Int): Query = {
    val query = select_distinct('taxon, 'taxon_label, 'score) where (
      bgp(
        t(gene, has_phenotypic_profile, 'gene_profile),
        t('comparison, for_query_profile, 'gene_profile),
        t('comparison, combined_score, 'score),
        t('comparison, for_corpus_profile, 'taxon_profile),
        t('taxon, has_phenotypic_profile, 'taxon_profile),
        t('taxon, rdfsLabel, 'taxon_label))) order_by desc('score) limit resultLimit
    query.setOffset(resultOffset)
    query
  }

  def comparisonSubsumersQuery(gene: IRI, taxon: IRI): Query =
    select_distinct('subsumer, 'ic) where (
      bgp(
        t(gene, has_phenotypic_profile, 'gene_profile),
        t(taxon, has_phenotypic_profile, 'taxon_profile),
        t('comparison, for_query_profile, 'gene_profile),
        t('comparison, for_corpus_profile, 'taxon_profile),
        t('comparison, has_subsumer, 'subsumer),
        t('subsumer, has_ic, 'ic)))

  def subsumedAnnotationsQuery(instance: OWLNamedIndividual, subsumer: OWLClass): Query =
    select_distinct('annotation) where (
      bgp(
        t(instance, has_phenotypic_profile / rdfType, 'annotation),
        t('annotation, rdfsSubClassOf*, subsumer)))

  def constructMatchFor(gene: IRI): QuerySolution => SimilarityMatch =
    (result: QuerySolution) => SimilarityMatch(MinimalTerm(IRI.create(result.getResource("taxon").getURI), result.getLiteral("taxon_label").getLexicalForm),
      result.getLiteral("score").getDouble)

}

case class SimilarityMatch(corpusProfile: MinimalTerm, score: Double) extends JSONResultItem {

  def toJSON: JsObject = {
    Map(
      "match_profile" -> corpusProfile.toJSON,
      "score" -> score.toJson).toJson.asJsObject
  }

}

case class Subsumer(term: MinimalTerm, ic: Double) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("term" -> term.toJSON,
      "ic" -> ic.toJson).toJson.asJsObject
  }

}

object Subsumer {

  def fromQuery(result: QuerySolution): Future[Subsumer] = {
    val iri = IRI.create(result.getResource("subsumer").getURI)
    Term.computedLabel(iri).map(term => Subsumer(term, result.getLiteral("ic").getDouble))
  }

}