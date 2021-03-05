package org.phenoscape.kb

import akka.util.Timeout
import org.apache.jena.query.{Query, QuerySolution}
import org.phenoscape.kb.Facets.Facet
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.queries.QueryUtil.{PhenotypicQuality, QualitySpec}
import org.phenoscape.kb.queries.TaxonPhenotypes
import org.phenoscape.owl.Vocab._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.semanticweb.owlapi.model.IRI
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object Phenotype {

  implicit val timeout = Timeout(10 minutes)

  def info(phenotype: IRI, annotatedStatesOnly: Boolean): Future[Phenotype] = {
    val eqsFuture = eqForPhenotype(phenotype)
    val statesFuture = characterStatesForPhenotype(phenotype, annotatedStatesOnly)
    val labelFuture = Term.label(phenotype)
    for {
      eqs <- eqsFuture
      states <- statesFuture
      labelOpt <- labelFuture
    } yield Phenotype(phenotype, labelOpt.flatMap(_.label).getOrElse(""), states, eqs)
  }

  def characterStatesForPhenotype(phenotype: IRI, annotatedStatesOnly: Boolean): Future[Set[CharacterState]] = {
    val taxonConstraint = if (annotatedStatesOnly) sparql"?taxon $exhibits_state ?state ." else sparql""
    val query: QueryText =
      sparql"""
       SELECT DISTINCT ?character ?character_label ?state ?state_label ?matrix ?matrix_label
       FROM $KBMainGraph
       WHERE {
          ?state $describes_phenotype $phenotype .
          $taxonConstraint
          ?character $may_have_state_value ?state .
          ?matrix $has_character ?character .
          ?state $rdfsLabel ?state_label .
          ?character $rdfsLabel ?character_label .
          ?matrix $rdfsLabel ?matrix_label .
       }
            """
    App
      .executeSPARQLQueryString(
        query.text,
        solution =>
          CharacterState(
            IRI.create(solution.getResource("state").getURI),
            solution.getLiteral("state_label").getLexicalForm,
            MinimalTerm(
              IRI.create(solution.getResource("character").getURI),
              Some(solution.getLiteral("character_label").getLexicalForm)
            ),
            MinimalTerm(
              IRI.create(solution.getResource("matrix").getURI),
              Some(solution.getLiteral("matrix_label").getLexicalForm)
            )
          )
      )
      .map(_.toSet)
  }

  def eqForPhenotype(phenotype: IRI): Future[NearestEQSet] = {
    val entitiesFuture = entitiesForPhenotype(phenotype, has_part_inhering_in.getIRI)
    val generalEntitiesFuture = entitiesForPhenotype(phenotype, phenotype_of.getIRI)
    //FIXME need to change to relatedEntities using has_part_towards_some; this must be added to the KB build
    //val relatedEntitiesFuture = ???
    val qualitiesFuture = qualitiesForPhenotype(phenotype)
    for {
      entities <- entitiesFuture
      generalEntities <- generalEntitiesFuture
      qualities <- qualitiesFuture
    } yield NearestEQSet(entities,
                         qualities,
                         generalEntities -- entities
    ) //FIXME this is an approximation for using towards
  }

  def entitiesForPhenotype(phenotype: IRI, relation: IRI): Future[Set[IRI]] =
    for {
      entityTypesResult <- App.executeSPARQLQuery(phenotypeEntitiesQuery(phenotype, relation),
                                                  result => IRI.create(result.getResource("entity").getURI))
    } yield entityTypesResult.toSet

  def qualitiesForPhenotype(phenotype: IRI): Future[Set[IRI]] =
    for {
      qualities <- App.executeSPARQLQuery(phenotypeQualitiesQuery(phenotype),
                                          result => IRI.create(result.getResource("quality").getURI))
    } yield qualities.toSet

  def queryTaxonPhenotypes(entity: Option[IRI],
                           quality: QualitySpec,
                           inTaxonOpt: Option[IRI],
                           phenotypeOpt: Option[IRI],
                           publicationOpt: Option[IRI],
                           includeParts: Boolean,
                           includeHistoricalHomologs: Boolean,
                           includeSerialHomologs: Boolean,
                           limit: Int = 20,
                           offset: Int = 0): Future[Seq[MinimalTerm]] =
    for {
      query <- TaxonPhenotypes.buildQuery(entity,
                                          quality,
                                          inTaxonOpt,
                                          phenotypeOpt,
                                          publicationOpt,
                                          includeParts,
                                          includeHistoricalHomologs,
                                          includeSerialHomologs,
                                          false,
                                          limit,
                                          offset)
      phenotypes <- App.executeSPARQLQueryString(query, fromQueryResult)
    } yield phenotypes

  def queryTaxonPhenotypesTotal(entity: Option[IRI],
                                quality: QualitySpec,
                                inTaxonOpt: Option[IRI],
                                phenotypeOpt: Option[IRI],
                                publicationOpt: Option[IRI],
                                includeParts: Boolean,
                                includeHistoricalHomologs: Boolean,
                                includeSerialHomologs: Boolean): Future[Int] =
    for {
      query <- TaxonPhenotypes.buildQuery(entity,
                                          quality,
                                          inTaxonOpt,
                                          phenotypeOpt,
                                          publicationOpt,
                                          includeParts,
                                          includeHistoricalHomologs,
                                          includeSerialHomologs,
                                          true,
                                          0,
                                          0)
      result <- App.executeSPARQLQuery(query)
    } yield ResultCount.count(result)

  def facetPhenotypeByEntity(focalEntity: Option[IRI],
                             quality: QualitySpec,
                             inTaxonOpt: Option[IRI],
                             publicationOpt: Option[IRI],
                             includeParts: Boolean,
                             includeHistoricalHomologs: Boolean,
                             includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      queryTaxonPhenotypesTotal(Some(iri),
                                quality,
                                inTaxonOpt,
                                None,
                                publicationOpt,
                                includeParts,
                                includeHistoricalHomologs,
                                includeSerialHomologs)
    val refine = (iri: IRI) =>
      Term
        .queryAnatomySubClasses(iri, KBVocab.Uberon, includeParts, includeHistoricalHomologs, includeSerialHomologs)
        .map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine, false)
  }

  def facetPhenotypeByQuality(focalQuality: Option[IRI],
                              entity: Option[IRI],
                              inTaxonOpt: Option[IRI],
                              publicationOpt: Option[IRI],
                              includeParts: Boolean,
                              includeHistoricalHomologs: Boolean,
                              includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      queryTaxonPhenotypesTotal(entity,
                                PhenotypicQuality(Some(iri)),
                                inTaxonOpt,
                                None,
                                publicationOpt,
                                includeParts,
                                includeHistoricalHomologs,
                                includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine, false)
  }

  def facetPhenotypeByTaxon(focalTaxon: Option[IRI],
                            entity: Option[IRI],
                            quality: QualitySpec,
                            publicationOpt: Option[IRI],
                            includeParts: Boolean,
                            includeHistoricalHomologs: Boolean,
                            includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      queryTaxonPhenotypesTotal(entity,
                                quality,
                                Some(iri),
                                None,
                                publicationOpt,
                                includeParts,
                                includeHistoricalHomologs,
                                includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.VTO)).map(_.toSet)
    Facets.facet(focalTaxon.getOrElse(KBVocab.taxonRoot), query, refine, true)
  }

  private def fromQueryResult(result: QuerySolution): MinimalTerm =
    MinimalTerm(IRI.create(result.getResource("phenotype").getURI),
                Some(result.getLiteral("phenotype_label").getLexicalForm))

  private def phenotypeEntitiesQuery(phenotype: IRI, relation: IRI): Query =
    sparql"""
        SELECT DISTINCT ?entity
        FROM $KBMainGraph
        FROM $KBClosureGraph
        FROM $KBRedundantRelationGraph
        WHERE {
          $phenotype $relation ?entity .
          ?entity $rdfsIsDefinedBy $Uberon .
          FILTER NOT EXISTS {
            $phenotype $relation ?other_entity .
            ?other_entity $rdfsSubClassOf ?entity .
            ?other_entity $rdfsIsDefinedBy $Uberon .
            FILTER(?other_entity != ?entity)
          }
        }
        """.toQuery

  private def phenotypeQualitiesQuery(phenotype: IRI): Query =
    sparql"""
        SELECT DISTINCT ?quality
        FROM $KBMainGraph
        FROM $KBClosureGraph
        FROM $KBRedundantRelationGraph
        WHERE {
          $phenotype $has_part ?quality .
          ?quality $rdfsIsDefinedBy $PATO .
          FILTER NOT EXISTS {
            $phenotype $has_part ?other_quality .
            ?other_quality $rdfsSubClassOf ?quality .
            ?other_quality $rdfsIsDefinedBy $PATO .
            FILTER(?other_quality != ?quality)
          }
        }
        """.toQuery

}

final case class Phenotype(iri: IRI, label: String, states: Set[CharacterState], eqs: NearestEQSet)
    extends JSONResultItem {

  override def toJSON: JsObject =
    Map(
      "@id" -> iri.toString.toJson,
      "label" -> label.toJson,
      "states" -> states.map(_.toJSON).toJson,
      "eqs" -> eqs.toJSON
    ).toJson.asJsObject

}

final case class NearestEQSet(entities: Set[IRI], qualities: Set[IRI], relatedEntities: Set[IRI])
    extends JSONResultItem {

  override def toJSON: JsObject =
    Map(
      "entities" -> entities.map(_.toString).toSeq.sorted.toJson,
      "qualities" -> qualities.map(_.toString).toSeq.sorted.toJson,
      "related_entities" -> relatedEntities.map(_.toString).toSeq.sorted.toJson
    ).toJson.asJsObject

}
