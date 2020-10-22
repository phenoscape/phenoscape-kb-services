package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.{Query, QuerySolution}
import org.apache.jena.rdf.model.{Model, Property, Resource, ResourceFactory}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.expr.{E_NotOneOf, Expr, ExprList, ExprVar}
import org.apache.jena.sparql.syntax.ElementFilter
import org.phenoscape.kb.Facets.Facet
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.JSONResultItem.JSONResultItemsMarshaller
import org.phenoscape.kb.queries.GeneAffectingPhenotype
import org.phenoscape.owl.Vocab._
import org.phenoscape.owl.{NamedRestrictionGenerator, Vocab}
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLClassExpression, OWLObjectProperty}
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.implicitConversions

object Gene {

  private val factory = OWLManager.getOWLDataFactory

  def withIRI(iri: IRI): Future[Option[Gene]] =
    App.executeSPARQLQuery(buildGeneQuery(iri), Gene.fromIRIQuery(iri)).map(_.headOption)

  def search(text: String, taxonOpt: Option[IRI]): Future[Seq[MatchedTerm[Gene]]] = {
    val taxonFilter = taxonOpt.map(taxonIRI => (gene: Gene) => gene.taxon.iri == taxonIRI).getOrElse((_: Gene) => true)
    App
      .executeSPARQLQuery(buildSearchQuery(text), Gene(_))
      .map(genes => Term.orderBySearchedText(genes.filter(taxonFilter), text))
  }

  def query(entity: OWLClassExpression = owlThing,
            taxon: OWLClassExpression = owlThing,
            limit: Int = 20,
            offset: Int = 0): Future[Seq[Gene]] =
    for {
      query <- App.expandWithOwlet(buildQuery(entity, taxon, limit, offset))
      descriptions <- App.executeSPARQLQuery(query, Gene(_))
    } yield descriptions

  def queryTotal(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing): Future[ResultCount] =
    for {
      query <- App.expandWithOwlet(buildTotalQuery(entity, taxon))
      result <- App.executeSPARQLQuery(query)
    } yield ResultCount(result)

  def buildSearchQuery(text: String): Query = {
    val searchText = if (text.endsWith("*")) text else s"$text*"
    val query = select_distinct('gene, 'gene_label, 'taxon, 'taxon_label) from "http://kb.phenoscape.org/" where bgp(
      t('gene_label, BDSearch, NodeFactory.createLiteral(searchText)),
      t('gene_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
      t('gene_label, BDRank, 'rank),
      t('gene, rdfsLabel, 'gene_label),
      t('gene, rdfType, AnnotatedGene),
      t('gene, in_taxon, 'taxon),
      t('taxon, rdfsLabel, 'taxon_label)
    )
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(100)
    query
  }

  def buildBasicQuery(entity: OWLClassExpression = owlThing,
                      taxon: OWLClassExpression = owlThing,
                      limit: Int = 20,
                      offset: Int = 0): Query = {
    //TODO allowing expressions makes it impossible to look for absences using the entity as an Individual... fix?
    val entityPatterns =
      if (entity == owlThing) Nil
      else
        t('annotation, rdfType, 'phenotype) :: t(
          'phenotype,
          rdfsSubClassOf,
          ((has_part some (inheres_in some entity)) or (has_part some (towards some entity))).asOMN) :: Nil
    val taxonPatterns =
      if (taxon == owlThing) Nil
      else
        t('annotation, associated_with_taxon, 'taxon) :: t(
          'taxon,
          rdfsSubClassOf,
          taxon.asOMN) :: Nil //FIXME this is different for Monarch data
    val query = select_distinct() from "http://kb.phenoscape.org/" where bgp(
      t('annotation, rdfType, AnnotatedPhenotype) ::
        t('annotation, associated_with_gene, 'gene) ::
        t('gene, rdfsLabel, 'gene_label) ::
        t('gene, in_taxon, 'taxon) ::
        t('taxon, rdfsLabel, 'taxon_label) ::
        taxonPatterns ++
        entityPatterns: _*
    )
    query
  }

  def buildQuery(entity: OWLClassExpression = owlThing,
                 taxon: OWLClassExpression = owlThing,
                 limit: Int = 20,
                 offset: Int = 0): Query = {
    val query = buildBasicQuery(entity, taxon)
    query.addResultVar('gene)
    query.addResultVar('gene_label)
    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    query.setOffset(offset)
    if (limit > 0) query.setLimit(limit)
    query.addOrderBy('gene_label)
    query.addOrderBy('gene)
    query
  }

  def buildTotalQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing): Query = {
    val query = buildBasicQuery(entity, taxon)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("gene"))))
    query
  }

  def buildGeneQuery(iri: IRI): Query =
    sparql"""
      SELECT DISTINCT ?gene_label ?taxon ?taxon_label
      FROM $KBMainGraph
      WHERE {
        $iri $rdfsLabel ?gene_label .
        $iri $rdfType $AnnotatedGene .
        $iri $in_taxon ?taxon .
        ?taxon $rdfsLabel ?taxon_label .
      }
          """.toQuery

  def fromIRIQuery(iri: IRI)(result: QuerySolution): Gene =
    Gene(iri, Option(result.getLiteral("gene_label")).map(_.getLexicalForm), Taxon(result))

  def affectingPhenotypeOfEntity(entity: Option[IRI],
                                 quality: Option[IRI],
                                 includeParts: Boolean,
                                 includeHistoricalHomologs: Boolean,
                                 includeSerialHomologs: Boolean,
                                 limit: Int,
                                 offset: Int): Future[Seq[Gene]] =
    for {
      query <- GeneAffectingPhenotype.buildQuery(entity,
                                                 quality,
                                                 includeParts,
                                                 includeHistoricalHomologs,
                                                 includeSerialHomologs,
                                                 false,
                                                 limit,
                                                 offset)
      genes <- App.executeSPARQLQueryString(query, Gene(_))
    } yield genes

  def affectingPhenotypeOfEntityTotal(entity: Option[IRI],
                                      quality: Option[IRI],
                                      includeParts: Boolean,
                                      includeHistoricalHomologs: Boolean,
                                      includeSerialHomologs: Boolean): Future[Int] =
    for {
      query <-
        GeneAffectingPhenotype
          .buildQuery(entity, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
      total <- App.executeSPARQLQuery(query).map(ResultCount.count)
    } yield total

  def facetGenesWithPhenotypeByEntity(focalEntity: Option[IRI],
                                      quality: Option[IRI],
                                      includeParts: Boolean,
                                      includeHistoricalHomologs: Boolean,
                                      includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      affectingPhenotypeOfEntityTotal(Some(iri),
                                      quality,
                                      includeParts,
                                      includeHistoricalHomologs,
                                      includeSerialHomologs)
    val refine = (iri: IRI) =>
      Term
        .queryAnatomySubClasses(iri, KBVocab.Uberon, includeParts, includeHistoricalHomologs, includeSerialHomologs)
        .map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine, false)
  }

  def facetGenesWithPhenotypeByQuality(focalQuality: Option[IRI],
                                       entity: Option[IRI],
                                       includeParts: Boolean,
                                       includeHistoricalHomologs: Boolean,
                                       includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) =>
      affectingPhenotypeOfEntityTotal(entity, Some(iri), includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine, false)
  }

  def expressedWithinEntity(entity: IRI, limit: Int, offset: Int): Future[Seq[Gene]] = {
    val query = buildGeneExpressedInEntityQuery(entity)
    query.addResultVar('gene)
    query.addResultVar('gene_label)
    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('gene_label)
    query.addOrderBy('gene)
    App.executeSPARQLQuery(query, Gene(_))
  }

  def expressedWithinEntityTotal(entity: IRI): Future[Int] = {
    val query = buildGeneExpressedInEntityQuery(entity)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("gene"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def phenotypicProfile(iri: IRI): Future[Seq[SourcedMinimalTerm]] = {
    val query =
      sparql"""
              CONSTRUCT {
                $iri $hasAnnotation ?phenotype .
                ?phenotype $dcSource ?source .
              }
              FROM $KBMainGraph
              WHERE {
                ?association $rdfType $association ;
                             $associationHasSubject  $iri ;
                             $associationHasObject  ?phenotype ;
                             $associationHasPredicate $has_phenotype
                OPTIONAL {
                  ?association  $dcSource  ?source 
                }
                FILTER ( ?association NOT IN ($AnnotatedPhenotype, $owlNamedIndividual) )
              }
              """

    print("chutki" + query.toString)

    for {
      annotationsData <- App.executeSPARQLConstructQuery(query.toQuery)
      phenotypesWithSources = processProfileResultToAnnotationsAndSources(annotationsData)
      labelledPhenotypes <- Future.sequence(phenotypesWithSources.map {
        case (phenotype, sources) => Term.computedLabel(phenotype).map(SourcedMinimalTerm(_, sources))
      })
    } yield labelledPhenotypes.toSeq.sortBy(_.term.label.map(_.toLowerCase))
  }

  private val hasAnnotation = ResourceFactory.createProperty("http://example.org/hasAnnotation")

  implicit private def objectPropertyToJenaProperty(prop: OWLObjectProperty): Property =
    ResourceFactory.createProperty(prop.getIRI.toString)

  private def processProfileResultToAnnotationsAndSources(model: Model): Set[(IRI, Set[IRI])] =
    model
      .listObjectsOfProperty(hasAnnotation)
      .asScala
      .collect {
        case annotation: Resource => annotation
      }
      .map { annotation =>
        IRI.create(annotation.getURI) -> model
          .listObjectsOfProperty(annotation, dcSource)
          .asScala
          .collect {
            case resource: Resource => Option(resource.getURI)
          }
          .flatten
          .map(IRI.create)
          .toSet
      }
      .toSet

  def expressionProfile(iri: IRI): Future[Seq[SourcedMinimalTerm]] = {
    val query = construct(t(iri, hasAnnotation.asNode, 'entity),
                          t('entity, dcSource, 'source)) from "http://kb.phenoscape.org/" where (bgp(
      t('expression, rdfType, GeneExpression),
      t('expression, associated_with_gene, iri),
      t('expression, occurs_in, 'entity)),
    optional(bgp(t('expression, dcSource, 'source))),
    new ElementFilter(
      new E_NotOneOf(new ExprVar('entity),
                     new ExprList(
                       List[Expr](new NodeValueNode(owlNamedIndividual), new NodeValueNode(owlClass)).asJava))))
    for {
      annotationsData <- App.executeSPARQLConstructQuery(query)
      entitiesWithSources = processProfileResultToAnnotationsAndSources(annotationsData)
      labelledEntities <- Future.sequence(entitiesWithSources.map {
        case (entity, sources) => Term.computedLabel(entity).map(SourcedMinimalTerm(_, sources))
      })
    } yield labelledEntities.toSeq.sortBy(_.term.label.map(_.toLowerCase))
  }

  case class Phenotype(iri: IRI)

  private def formatResult(result: QuerySolution): String = {
    val gene = result.getResource("gene").getURI
    val geneLabel = result.getLiteral("gene_label").getLexicalForm
    val taxon = result.getLiteral("taxon_label").getLexicalForm
    val source = Option(result.getResource("source")).map(_.getURI).getOrElse("")
    s"$gene\t$geneLabel\t$taxon\t$source"
  }

  private def buildGeneExpressedInEntityQuery(entityIRI: IRI): Query = {
    val partOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)
    //FIXME check that * gets overridden
    sparql"""
      SELECT DISTINCT *
      FROM $KBMainGraph
      FROM $KBClosureGraph
      WHERE {
        ?gene $rdfsLabel ?gene_label .
        ?expression $associated_with_gene ?gene .
        ?expression $occurs_in/$rdfsSubClassOf/$partOfSome $entityIRI .
        ?gene $in_taxon ?taxon .
        ?taxon $rdfsLabel ?taxon_label .
      }
          """.toQuery
  }

  def apply(result: QuerySolution): Gene = {
    val geneURI = result.getResource("gene").getURI
    Gene(IRI.create(geneURI), Option(result.getLiteral("gene_label")).map(_.getLexicalForm), Taxon(result))
  }

  implicit val GenesTextMarshaller: ToEntityMarshaller[Seq[Gene]] =
    Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { genes =>
      val header = "IRI\tlabel\ttaxon"
      s"$header\n${genes.map(_.toString).mkString("\n")}"
    }

  implicit val ComboGenesMarshaller: ToEntityMarshaller[Seq[Gene]] =
    Marshaller.oneOf(GenesTextMarshaller, JSONResultItemsMarshaller)

}

case class Gene(iri: IRI, label: Option[String], taxon: Taxon) extends LabeledTerm with JSONResultItem {

  def toJSON: JsObject =
    Map("@id" -> iri.toString.toJson,
        "label" -> label.map(_.toJson).getOrElse(JsNull),
        "taxon" -> taxon.toJSON).toJson.asJsObject

  override def toString: String =
    s"$iri\t$label\t${taxon.label}"

}
