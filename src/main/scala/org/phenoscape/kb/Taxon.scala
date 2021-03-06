package org.phenoscape.kb

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.postfixOps
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.{E_Coalesce, Expr, ExprList, ExprVar}
import org.apache.jena.sparql.path.{P_Link, P_OneOrMore1}
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueString
import org.apache.jena.sparql.syntax.ElementBind
import org.apache.jena.sparql.syntax.ElementNamedGraph
import org.apache.jena.sparql.syntax.ElementSubQuery
import org.apache.jena.vocabulary.RDFS
import org.phenoscape.kb.Facets.Facet
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.JSONResultItem.JSONResultItemsMarshaller
import org.phenoscape.kb.queries.DirectPhenotypesForTaxon
import org.phenoscape.kb.queries.TaxaWithPhenotype
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.semanticweb.owlapi.model.{IRI, OWLClassExpression, OWLEntity, OWLObject}
import org.semanticweb.owlapi.model.OWLClassExpression
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes
import org.phenoscape.kb.queries.QueryUtil.{PhenotypicQuality, QualitySpec}
import spray.json._
import spray.json.DefaultJsonProtocol._

object Taxon {

  val phylopic = ObjectProperty("http://purl.org/phenoscape/phylopics.owl#phylopic")
  val group_label = ObjectProperty("http://purl.org/phenoscape/phylopics.owl#group_label")
  val is_extinct = ObjectProperty("http://purl.obolibrary.org/obo/vto#is_extinct")
  val has_rank = ObjectProperty("http://purl.obolibrary.org/obo/vto#has_rank")

  def withIRI(iri: IRI): Future[Option[TaxonInfo]] = {
    def fromIRIQuery(result: QuerySolution) =
      (
        iri,
        result.getLiteral("label").getLexicalForm,
        Option(result.getLiteral("rank_label")).map(label =>
          MinimalTerm(IRI.create(result.getResource("rank").getURI), Some(label.getLexicalForm))),
        Option(result.getLiteral("common_name")).map(_.getString),
        Option(result.getLiteral("is_extinct")).map(_.getBoolean).getOrElse(false)
      )

    val taxonFuture = App.executeSPARQLQuery(buildTaxonQuery(iri), fromIRIQuery).map(_.headOption)
    val synonymsFuture = taxonSynonyms(iri)

    for {
      taxon <- taxonFuture
      synonyms <- synonymsFuture
    } yield taxon.map { case (iri, label, rank_label, common_name, is_extinct) =>
      TaxonInfo(iri, label, rank_label, common_name, synonyms, is_extinct)
    }
  }

  def withPhenotypeExpression(entity: OWLClassExpression = OWLThing,
                              quality: OWLClassExpression = OWLThing,
                              inTaxonOpt: Option[IRI],
                              publicationOpt: Option[IRI],
                              includeParts: Boolean,
                              includeHistoricalHomologs: Boolean,
                              includeSerialHomologs: Boolean,
                              limit: Int = 20,
                              offset: Int = 0): Future[Seq[Taxon]] =
    for {
      query <- buildTaxaWithPhenotypeQuery(entity,
                                           quality,
                                           inTaxonOpt,
                                           includeParts,
                                           includeHistoricalHomologs,
                                           includeSerialHomologs,
                                           limit,
                                           offset)
      taxa <- App.executeSPARQLQuery(query, Taxon(_))
    } yield taxa

  def withPhenotype(entity: Option[IRI],
                    quality: QualitySpec,
                    inTaxonOpt: Option[IRI],
                    phenotypeOpt: Option[IRI],
                    publicationOpt: Option[IRI],
                    includeParts: Boolean,
                    includeHistoricalHomologs: Boolean,
                    includeSerialHomologs: Boolean,
                    limit: Int = 20,
                    offset: Int = 0): Future[Seq[Taxon]] = {
    val entityOpt = entity.filterNot(_ == OWLThing.getIRI) //FIXME do this filter in caller
    //val qualityIRI = Option(quality).filterNot(_ == OWLThing.getIRI)
    for {
      query <- TaxaWithPhenotype.buildQuery(entityOpt,
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
      taxa <- App.executeSPARQLQueryString(query, Taxon(_))
    } yield taxa
  }

  def withPhenotypeExpressionTotal(entity: OWLClassExpression = OWLThing,
                                   quality: OWLClassExpression = OWLThing,
                                   inTaxonOpt: Option[IRI],
                                   publicationOpt: Option[IRI],
                                   includeParts: Boolean,
                                   includeHistoricalHomologs: Boolean,
                                   includeSerialHomologs: Boolean): Future[Int] =
    for {
      query <- buildTaxaWithPhenotypeTotalQuery(entity,
                                                quality,
                                                inTaxonOpt,
                                                includeParts,
                                                includeHistoricalHomologs,
                                                includeSerialHomologs)
      result <- App.executeSPARQLQuery(query)
    } yield ResultCount.count(result)

  def withPhenotypeTotal(entity: Option[IRI],
                         quality: QualitySpec,
                         inTaxonOpt: Option[IRI],
                         phenotypeOpt: Option[IRI],
                         publicationOpt: Option[IRI],
                         includeParts: Boolean,
                         includeHistoricalHomologs: Boolean,
                         includeSerialHomologs: Boolean): Future[Int] = {
    val entityOpt = entity.filterNot(_ == OWLThing.getIRI) //FIXME do this filter in caller
    for {
      query <- TaxaWithPhenotype.buildQuery(entityOpt,
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
  }

  def facetTaxaWithPhenotypeByEntity(focalEntity: Option[IRI],
                                     quality: QualitySpec,
                                     inTaxonOpt: Option[IRI],
                                     publicationOpt: Option[IRI],
                                     includeParts: Boolean,
                                     includeHistoricalHomologs: Boolean,
                                     includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      withPhenotypeTotal(Some(iri),
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

  def facetTaxaWithPhenotypeByQuality(focalQuality: Option[IRI],
                                      entity: Option[IRI],
                                      inTaxonOpt: Option[IRI],
                                      publicationOpt: Option[IRI],
                                      includeParts: Boolean,
                                      includeHistoricalHomologs: Boolean,
                                      includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      withPhenotypeTotal(entity,
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

  def facetTaxaWithPhenotypeByTaxon(focalTaxon: Option[IRI],
                                    entity: Option[IRI],
                                    quality: QualitySpec,
                                    publicationOpt: Option[IRI],
                                    includeParts: Boolean,
                                    includeHistoricalHomologs: Boolean,
                                    includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      withPhenotypeTotal(entity,
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

  private def facetResultToMap(facets: List[(MinimalTerm, Int)]) =
    Map("facets" -> facets.map { case (term, count) => Map("term" -> term, "count" -> count) })

  def variationProfileFor(taxon: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[AnnotatedCharacterDescription]] = {
    val results = App.executeSPARQLQuery(
      buildVariationProfileQuery(taxon, limit, offset),
      result =>
        Term.computedLabel(IRI.create(result.getResource("phenotype").getURI)).map { phenotype =>
          AnnotatedCharacterDescription(
            CharacterDescription(
              IRI.create(result.getResource("state").getURI),
              result.getLiteral("description").getLexicalForm,
              CharacterMatrix(IRI.create(result.getResource("matrix").getURI),
                              result.getLiteral("matrix_label").getLexicalForm),
              MinimalTerm(IRI.create(result.getResource("character").getURI),
                          Some(result.getLiteral("characterLabel").getLexicalForm))
            ),
            phenotype
          )
        }
    )
    results.flatMap(Future.sequence(_))
  }

  def variationProfileTotalFor(taxon: IRI): Future[Int] =
    App.executeSPARQLQuery(buildVariationProfileTotalQuery(taxon)).map(ResultCount.count)

  def commonGroupFor(taxon: IRI): Future[Option[CommonGroup]] =
    App.executeSPARQLQuery(buildPhylopicQuery(taxon), CommonGroup(_)).map(_.headOption)

  def directPhenotypesForExpression(taxon: IRI,
                                    entityOpt: Option[OWLClassExpression],
                                    qualityOpt: Option[OWLClassExpression],
                                    includeParts: Boolean,
                                    includeHistoricalHomologs: Boolean,
                                    includeSerialHomologs: Boolean,
                                    limit: Int = 20,
                                    offset: Int = 0): Future[Seq[AnnotatedCharacterDescription]] = {
    val queryFuture = for {
      rawQuery <- buildPhenotypesSubQuery(taxon,
                                          entityOpt,
                                          qualityOpt,
                                          includeParts,
                                          includeHistoricalHomologs,
                                          includeSerialHomologs)
    } yield {
      val query = rawQuery from "http://kb.phenoscape.org/"
      if (limit > 1) {
        query.setOffset(offset)
        query.setLimit(limit)
      }
      query.addOrderBy('description)
      query.addOrderBy('phenotype)
      query
    }
    val results = for {
      query <- queryFuture
      result <- App.executeSPARQLQuery(query, AnnotatedCharacterDescription.fromQuerySolution)
    } yield result
    results.flatMap(Future.sequence(_))
  }

  def directPhenotypesFor(taxon: IRI,
                          entityOpt: Option[IRI],
                          quality: QualitySpec,
                          phenotypeOpt: Option[IRI],
                          includeParts: Boolean,
                          includeHistoricalHomologs: Boolean,
                          includeSerialHomologs: Boolean,
                          limit: Int = 20,
                          offset: Int = 0): Future[Seq[AnnotatedCharacterDescription]] = {
    val results = for {
      query <- DirectPhenotypesForTaxon.buildQuery(taxon,
                                                   entityOpt,
                                                   quality,
                                                   phenotypeOpt,
                                                   includeParts,
                                                   includeHistoricalHomologs,
                                                   includeSerialHomologs,
                                                   false,
                                                   limit,
                                                   offset)
      result <- App.executeSPARQLQueryString(query, AnnotatedCharacterDescription.fromQuerySolution)
    } yield result
    results.flatMap(Future.sequence(_))
  }

  def directPhenotypesTotalForExpression(taxon: IRI,
                                         entityOpt: Option[OWLClassExpression],
                                         qualityOpt: Option[OWLClassExpression],
                                         includeParts: Boolean,
                                         includeHistoricalHomologs: Boolean,
                                         includeSerialHomologs: Boolean): Future[Int] =
    for {
      rawQuery <- buildPhenotypesSubQuery(taxon,
                                          entityOpt,
                                          qualityOpt,
                                          includeParts,
                                          includeHistoricalHomologs,
                                          includeSerialHomologs)
      query = select() from "http://kb.phenoscape.org/" where (new ElementSubQuery(rawQuery))
      _ = query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
      result <- App.executeSPARQLQuery(query).map(ResultCount.count)
    } yield result

  def directPhenotypesTotalFor(taxon: IRI,
                               entityOpt: Option[IRI],
                               quality: QualitySpec,
                               phenotypeOpt: Option[IRI],
                               includeParts: Boolean,
                               includeHistoricalHomologs: Boolean,
                               includeSerialHomologs: Boolean): Future[Int] =
    for {
      query <- DirectPhenotypesForTaxon.buildQuery(taxon,
                                                   entityOpt,
                                                   quality,
                                                   phenotypeOpt,
                                                   includeParts,
                                                   includeHistoricalHomologs,
                                                   includeSerialHomologs,
                                                   true,
                                                   0,
                                                   0)
      result <- App.executeSPARQLQuery(query).map(ResultCount.count)
    } yield result

  private def buildPhenotypesSubQuery(taxon: IRI,
                                      entityOpt: Option[OWLClassExpression],
                                      qualityOpt: Option[OWLClassExpression],
                                      includeParts: Boolean,
                                      includeHistoricalHomologs: Boolean,
                                      includeSerialHomologs: Boolean): Future[Query] = {
    val phenotypePattern = if (entityOpt.nonEmpty || qualityOpt.nonEmpty) {
      val entity = entityOpt.getOrElse(owlThing)
      val quality = qualityOpt.getOrElse(owlThing)
      val actualEntity = (includeHistoricalHomologs, includeSerialHomologs) match {
        case (false, false) => entity
        case (true, false)  => entity or (homologous_to some entity)
        case (false, true)  => entity or (serially_homologous_to some entity)
        case (true, true)   => entity or (homologous_to some entity) or (serially_homologous_to some entity)
      }
      val entityExpression = if (includeParts) (actualEntity or (part_of some actualEntity)) else actualEntity
      t('phenotype, rdfsSubClassOf, ((has_part some quality) and (phenotype_of some entityExpression)).asOMN) :: Nil
    } else Nil
    val query = select_distinct('state, 'description, 'matrix, 'matrix_label, 'phenotype) where (
      bgp(
        t(taxon, exhibits_state, 'state) ::
          t('state, describes_phenotype, 'phenotype) ::
          t('state, dcDescription, 'description) ::
          t('matrix, has_character, 'character) ::
          t('character, may_have_state_value, 'state) ::
          t('character, rdfsLabel, 'character_label) ::
          t('matrix, rdfsLabel, 'matrix_label) ::
          phenotypePattern: _*
      )
    )
    App.expandWithOwlet(query)
  }

  def phyloPicAcknowledgments: Future[Seq[IRI]] = {
    val query = select_distinct('pic) where (bgp(t('subject, phylopic, 'pic)))
    App.executeSPARQLQuery(query, result => IRI.create(result.getResource("pic").getURI))
  }

  def taxaWithRank(rank: IRI, inTaxon: IRI): Future[Seq[MinimalTerm]] = {
    val query =
      select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (bgp(t('term, has_rank, rank),
                                                                                      t('term, rdfsLabel, 'term_label)),
      new ElementNamedGraph(NodeFactory.createURI("http://kb.phenoscape.org/closure"),
                            bgp(t('term, rdfsSubClassOf, inTaxon))))
    App.executeSPARQLQuery(query, MinimalTerm.fromQuerySolution)
  }

  def countOfAnnotatedTaxa(inTaxon: IRI): Future[Int] = {
    val query =
      select() from "http://kb.phenoscape.org/" where (bgp(t('taxon, exhibits_state / describes_phenotype, 'phenotype)),
      new ElementNamedGraph(NodeFactory.createURI("http://kb.phenoscape.org/closure"),
                            bgp(t('taxon, rdfsSubClassOf, inTaxon))))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  //  private def buildBasicQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Query = {
  //    val entityPatterns = if (entity == owlThing) Nil else
  //      t('phenotype, ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
  //    val (publicationFilters, publicationPatterns) = if (publications.isEmpty) (Nil, Nil) else
  //      (new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil,
  //        t('matrix, has_character / may_have_state_value, 'state) :: Nil)
  //    val taxonPatterns = if (taxon == owlThing) Nil else
  //      t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil
  //    select_distinct() from "http://kb.phenoscape.org/" where (
  //      bgp(
  //        t('state, describes_phenotype, 'phenotype) ::
  //          t('taxon, exhibits_state, 'state) ::
  //          t('taxon, rdfsLabel, 'taxon_label) ::
  //          entityPatterns ++
  //          publicationPatterns ++
  //          taxonPatterns: _*) ::
  //        publicationFilters: _*)
  //  }

  private def buildBasicTaxaWithPhenotypeQuery(entity: OWLClassExpression = owlThing,
                                               quality: OWLClassExpression = owlThing,
                                               inTaxonOpt: Option[IRI],
                                               includeParts: Boolean,
                                               includeHistoricalHomologs: Boolean,
                                               includeSerialHomologs: Boolean): Future[Query] = {
    val actualEntity = (includeHistoricalHomologs, includeSerialHomologs) match {
      case (false, false) => entity
      case (true, false)  => entity or (homologous_to some entity)
      case (false, true)  => entity or (serially_homologous_to some entity)
      case (true, true)   => entity or (homologous_to some entity) or (serially_homologous_to some entity)
    }
    val entityExpression = if (includeParts) (actualEntity or (part_of some actualEntity)) else actualEntity
    val taxonPatterns = inTaxonOpt.map(t('taxon, rdfsSubClassOf *, _)).toList
    val query = select_distinct('taxon, 'taxon_label) where (
      bgp(
        App.BigdataAnalyticQuery ::
          t('state, describes_phenotype, 'phenotype) ::
          t('taxon, exhibits_state, 'state) ::
          t('taxon, rdfsLabel, 'taxon_label) ::
          t('phenotype, rdfsSubClassOf, ((has_part some quality) and (phenotype_of some entityExpression)).asOMN) ::
          taxonPatterns: _*
      )
    )
    App.expandWithOwlet(query)
  }

  def buildTaxaWithPhenotypeQuery(entity: OWLClassExpression,
                                  quality: OWLClassExpression,
                                  inTaxonOpt: Option[IRI],
                                  includeParts: Boolean,
                                  includeHistoricalHomologs: Boolean,
                                  includeSerialHomologs: Boolean,
                                  limit: Int = 20,
                                  offset: Int = 0): Future[Query] =
    for {
      rawQuery <- buildBasicTaxaWithPhenotypeQuery(entity,
                                                   quality,
                                                   inTaxonOpt,
                                                   includeParts,
                                                   includeHistoricalHomologs,
                                                   includeSerialHomologs)
    } yield {
      val query = rawQuery from "http://kb.phenoscape.org/"
      query.setOffset(offset)
      if (limit > 0) query.setLimit(limit)
      query.addOrderBy('taxon_label)
      query.addOrderBy('taxon)
      query
    }

  def buildTaxaWithPhenotypeTotalQuery(entity: OWLClassExpression,
                                       quality: OWLClassExpression,
                                       inTaxonOpt: Option[IRI],
                                       includeParts: Boolean,
                                       includeHistoricalHomologs: Boolean,
                                       includeSerialHomologs: Boolean): Future[Query] =
    for {
      rawQuery <- buildBasicTaxaWithPhenotypeQuery(entity,
                                                   quality,
                                                   inTaxonOpt,
                                                   includeParts,
                                                   includeHistoricalHomologs,
                                                   includeSerialHomologs)
    } yield {
      val query = select() from "http://kb.phenoscape.org/" where (new ElementSubQuery(rawQuery))
      query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
      query
    }

  def taxonSynonyms(iri: IRI): Future[Seq[(IRI, String)]] = {
    val query =
      sparql"""
      SELECT DISTINCT ?relation ?synonym
      FROM $KBMainGraph
      WHERE {
        VALUES ?relation {
      		  $hasExactSynonym
      		  $hasRelatedSynonym
      		  $hasNarrowSynonym
      		  $hasBroadSynonym
          }
    		  $iri ?relation ?synonym .
      }
      """
    App.executeSPARQLQueryString(
      query.text,
      result => IRI.create(result.getResource("relation").getURI) -> result.getLiteral("synonym").getLexicalForm)
  }

  def buildTaxonQuery(iri: IRI): Query =
    select_distinct('label, 'is_extinct, 'rank, 'rank_label, 'common_name) from "http://kb.phenoscape.org/" where (bgp(
      t(iri, rdfsLabel, 'label),
      t(iri, rdfsIsDefinedBy, VTO)),
    optional(bgp(t(iri, is_extinct, 'is_extinct))),
    optional(bgp(t(iri, has_rank, 'rank), t('rank, rdfsLabel, 'rank_label))),
    optional(
      bgp(
        t('common_name_axiom, owlAnnotatedSource, iri),
        t('common_name_axiom, owlAnnotatedProperty, hasRelatedSynonym),
        t('common_name_axiom, owlAnnotatedTarget, 'common_name),
        t('common_name_axiom, hasSynonymType, CommonNameSynonymType)
      )
    ))

  def buildVariationProfileTotalQuery(taxon: IRI): Query = {
    val query =
      select() from "http://kb.phenoscape.org/" where (new ElementSubQuery(buildBasicVariationProfileQuery(taxon)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
    query
  }

  def buildVariationProfileQuery(taxon: IRI, limit: Int, offset: Int): Query = {
    val query = buildBasicVariationProfileQuery(taxon) from "http://kb.phenoscape.org/"
    if (limit > 0) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('description)
    query.addOrderBy('phenotype)
    query
  }

  def buildBasicVariationProfileQuery(taxon: IRI): Query = {
    val hasPhenotypicProfile = ObjectProperty(has_phenotypic_profile)
    select_distinct('state, 'description, 'matrix, 'matrix_label, 'phenotype) where (
      bgp(
        t(taxon, hasPhenotypicProfile / rdfType, 'phenotype),
        t('state, describes_phenotype, 'phenotype),
        t('state, dcDescription, 'description),
        t('matrix, has_character / may_have_state_value, 'state),
        t('matrix, rdfsLabel, 'matrix_label)
      )
    )
  }

  def buildPhylopicQuery(taxon: IRI): Query = {
    val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
    val query =
      select('super,
             'label,
             'picOpt) from "http://kb.phenoscape.org/" from "http://purl.org/phenoscape/phylopics.owl" where (bgp(
        t(taxon, rdfsSubClassOf *, 'super),
        t('super, group_label, 'label),
        t('super, rdfsSubClassOf *, 'ancestor)),
      optional(bgp(t('super, phylopic, 'pic))),
      new ElementBind('picOpt,
                      new E_Coalesce(
                        new ExprList(Seq[Expr](new ExprVar("pic"), new NodeValueString("")).asJava)))) order_by desc(
        'level) limit 1
    query.addGroupBy('super)
    query.addGroupBy('label)
    query.addGroupBy('picOpt)
    query.getProject.add(Var.alloc("level"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("ancestor"))))
    query
  }

  def apply(result: QuerySolution): Taxon =
    Taxon(IRI.create(result.getResource("taxon").getURI), result.getLiteral("taxon_label").getLexicalForm)

  def newickTreeWithRoot(iri: IRI): Future[String] = {
    val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
    val query = construct(t('child, rdfsSubClassOf, 'parent),
                          t('child, rdfsLabel, 'label)) from "http://kb.phenoscape.org/" where (
      bgp(
        t('parent, rdfsSubClassOf *, iri),
        t('child, rdfsSubClassOf, 'parent),
        t('child, rdfsLabel, 'label),
        t('child, rdfsIsDefinedBy, VTO),
        t('parent, rdfsIsDefinedBy, VTO)
      )
    )
    for {
      model <- App.executeSPARQLConstructQuery(query)
      taxon <- Term.computedLabel(iri)
    } yield {
      val taxonResource = ResourceFactory.createResource(iri.toString)
      taxon.label.foreach { label =>
        model.add(taxonResource, RDFS.label, label)
      }
      s"${newickFor(taxonResource, model)};"
    }
  }

  private def newickFor(parent: Resource, model: Model): String = {
    val reserved = Set(';', ',', ':', '(', ')', ' ', '"')
    val parentLabel = model.getProperty(parent, RDFS.label).getLiteral.getLexicalForm
    //val escapedLabel = if (parentLabel.exists(reserved)) s"'$parentLabel'" else parentLabel
    val escapedLabel = s"'${parentLabel.replaceAllLiterally("'", "\"")}'"
    val parentCount = model.listObjectsOfProperty(parent, RDFS.subClassOf).asScala.size
    if (parentCount > 1) println(s"WARNING: $parentCount parents for $parent")
    val children = model.listResourcesWithProperty(RDFS.subClassOf, parent).asScala.toSeq
    val childList = children.map(newickFor(_, model)).mkString(", ")
    val subtree = if (children.isEmpty) "" else s"($childList)"
    s"$subtree$escapedLabel"
  }

  val TaxaTextMarshaller: ToEntityMarshaller[Seq[Taxon]] =
    Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { taxa =>
      val header = "IRI\tlabel"
      s"$header\n${taxa.map(_.toString).mkString("\n")}"
    }

  implicit val ComboTaxaMarshaller = Marshaller.oneOf(TaxaTextMarshaller, JSONResultItemsMarshaller)

}

case class CommonGroup(label: String, phylopic: Option[IRI]) extends JSONResultItem {

  def toJSON: JsObject = {
    val pic = phylopic.map(iri => Map("phylopic" -> iri.toString)).getOrElse(Map.empty)
    (Map("label" -> label) ++ pic).toJson.asJsObject
  }

}

object CommonGroup {

  def apply(result: QuerySolution): CommonGroup =
    CommonGroup(result.getLiteral("label").getLexicalForm, {
                  val picOpt = result.get("picOpt")
                  if (picOpt.isURIResource) Some(IRI.create(picOpt.asResource.getURI)) else None
                })

}

case class Taxon(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject =
    Map("@id" -> iri.toString.toJson, "label" -> label.toJson).toJson.asJsObject

  override def toString(): String =
    s"$iri\t$label"

}

case class TaxonInfo(iri: IRI,
                     label: String,
                     rank: Option[MinimalTerm],
                     commonName: Option[String],
                     synonyms: Seq[(IRI, String)],
                     extinct: Boolean)
    extends JSONResultItem {

  // removing synonyms from output, as the list is a problem for R dataframes
//  "synonyms" -> synonyms.map {
//    case (iri, value) =>
//      JsObject("property" -> iri.toString.toJson, "value" -> value.toJson).toJson
//  }.toJson,

  def toJSON: JsObject =
    (Map("@id" -> iri.toString.toJson, "label" -> label.toJson, "extinct" -> extinct.toJson) ++
      rank.map("rank" -> _.toJSON) ++
      commonName.map("common_name" -> _.toJson)).toJson.asJsObject

}
