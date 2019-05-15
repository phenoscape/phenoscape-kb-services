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
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.implicitConversions

object Gene {

  private val factory = OWLManager.getOWLDataFactory

  //FIXME this is a temporary hack until genes are directly associated with taxa in the KB
  private val geneIDPrefixToTaxon = Map(
    "http://zfin.org" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_7955"), "Danio rerio"),
    "http://www.informatics.jax.org" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_10090"), "Mus musculus"),
    "http://xenbase.org" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_8353"), "Xenopus"),
    "http://www.ncbi.nlm.nih.gov" -> Taxon(IRI.create("http://purl.obolibrary.org/obo/NCBITaxon_9606"), "Homo sapiens"))

  def withIRI(iri: IRI): Future[Option[Gene]] =
    App.executeSPARQLQuery(buildGeneQuery(iri), Gene.fromIRIQuery(iri)).map(_.headOption)

  def search(text: String, taxonOpt: Option[IRI]): Future[Seq[MatchedTerm[Gene]]] = {
    val taxonFilter = taxonOpt.map(taxonIRI =>
      (gene: Gene) => gene.taxon.iri == taxonIRI).getOrElse((_: Gene) => true)
    App.executeSPARQLQuery(buildSearchQuery(text), Gene(_)).map(genes => Term.orderBySearchedText(genes.filter(taxonFilter), text))
  }

  def query(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, limit: Int = 20, offset: Int = 0): Future[Seq[Gene]] = for {
    query <- App.expandWithOwlet(buildQuery(entity, taxon, limit, offset))
    descriptions <- App.executeSPARQLQuery(query, Gene(_))
  } yield {
    descriptions
  }

  def queryTotal(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing): Future[ResultCount] = for {
    query <- App.expandWithOwlet(buildTotalQuery(entity, taxon))
    result <- App.executeSPARQLQuery(query)
  } yield {
    ResultCount(result)
  }

  def buildSearchQuery(text: String): Query = {
    val searchText = if (text.endsWith("*")) text else s"$text*"
    val query = select_distinct('gene, 'gene_label) from "http://kb.phenoscape.org/" where bgp(
      t('gene_label, BDSearch, NodeFactory.createLiteral(searchText)),
      t('gene_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
      t('gene_label, BDRank, 'rank),
      t('gene, rdfsLabel, 'gene_label),
      t('gene, rdfType, Vocab.Gene))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(100)
    query
  }

  def buildBasicQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, limit: Int = 20, offset: Int = 0): Query = {
    //TODO allowing expressions makes it impossible to look for absences using the entity as an Individual... fix?
    val entityPatterns = if (entity == owlThing) Nil else
      t('annotation, rdfType, 'phenotype) :: t('phenotype, rdfsSubClassOf, ((has_part some (inheres_in some entity)) or (has_part some (towards some entity))).asOMN) :: Nil
    val taxonPatterns = if (taxon == owlThing) Nil else
      t('annotation, associated_with_taxon, 'taxon) :: t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where bgp(
      t('annotation, rdfType, AnnotatedPhenotype) ::
        t('annotation, associated_with_gene, 'gene) ::
        t('gene, rdfsLabel, 'gene_label) ::
        taxonPatterns ++
          entityPatterns: _*)
    query
  }

  def buildQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, limit: Int = 20, offset: Int = 0): Query = {
    val query = buildBasicQuery(entity, taxon)
    query.addResultVar('gene)
    query.addResultVar('gene_label)
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
    select('gene_label) from "http://kb.phenoscape.org/" where bgp(
      t(iri, rdfsLabel, 'gene_label),
      t(iri, rdfType, Vocab.Gene))

  def fromIRIQuery(iri: IRI)(result: QuerySolution): Gene = Gene(
    iri,
    result.getLiteral("gene_label").getLexicalForm,
    taxonForGeneIRI(iri))

  def affectingPhenotypeOfEntity(entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean, limit: Int, offset: Int): Future[Seq[Gene]] = for {
    query <- GeneAffectingPhenotype.buildQuery(entity, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, false, limit, offset)
    genes <- App.executeSPARQLQueryString(query, Gene(_))
  } yield genes

  def affectingPhenotypeOfEntityTotal(entity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[Int] = for {
    query <- GeneAffectingPhenotype.buildQuery(entity, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, true, 0, 0)
    total <- App.executeSPARQLQuery(query).map(ResultCount.count)
  } yield total

  def facetGenesWithPhenotypeByEntity(focalEntity: Option[IRI], quality: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => affectingPhenotypeOfEntityTotal(Some(iri), quality, includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.queryAnatomySubClasses(iri, KBVocab.Uberon, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(_.toSet)
    Facets.facet(focalEntity.getOrElse(KBVocab.entityRoot), query, refine, false)
  }

  def facetGenesWithPhenotypeByQuality(focalQuality: Option[IRI], entity: Option[IRI], includeParts: Boolean, includeHistoricalHomologs: Boolean, includeSerialHomologs: Boolean): Future[List[Facet]] = {
    val query = (iri: IRI) => affectingPhenotypeOfEntityTotal(entity, Some(iri), includeParts, includeHistoricalHomologs, includeSerialHomologs)
    val refine = (iri: IRI) => Term.querySubClasses(iri, Some(KBVocab.PATO)).map(_.toSet)
    Facets.facet(focalQuality.getOrElse(KBVocab.qualityRoot), query, refine, false)
  }

  def expressedWithinEntity(entity: IRI, limit: Int, offset: Int): Future[Seq[Gene]] = {
    val query = buildGeneExpressedInEntityQuery(entity)
    query.addResultVar('gene)
    query.addResultVar('gene_label)
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
    val query = construct(
      t(iri, hasAnnotation.asNode, 'annotation),
      t('annotation, dcSource, 'source)) from "http://kb.phenoscape.org/" where(
      bgp(
        t('pheno_instance, rdfType, AnnotatedPhenotype),
        t('pheno_instance, associated_with_gene, iri),
        t('pheno_instance, rdfType, 'annotation)),
      optional(bgp(t('pheno_instance, dcSource, 'source))),
      new ElementFilter(new E_NotOneOf(new ExprVar('annotation), new ExprList(List[Expr](
        new NodeValueNode(AnnotatedPhenotype),
        new NodeValueNode(owlNamedIndividual)).asJava))))
    for {
      annotationsData <- App.executeSPARQLConstructQuery(query)
      phenotypesWithSources = processProfileResultToAnnotationsAndSources(annotationsData)
      labelledPhenotypes <- Future.sequence(phenotypesWithSources.map {
        case (phenotype, sources) => Term.computedLabel(phenotype).map(SourcedMinimalTerm(_, sources))
      })
    } yield labelledPhenotypes.toSeq.sortBy(_.term.label.toLowerCase)
  }

  private val hasAnnotation = ResourceFactory.createProperty("http://example.org/hasAnnotation")

  private implicit def objectPropertyToJenaProperty(prop: OWLObjectProperty): Property = {
    ResourceFactory.createProperty(prop.getIRI.toString)
  }

  private def processProfileResultToAnnotationsAndSources(model: Model): Set[(IRI, Set[IRI])] = {
    model.listObjectsOfProperty(hasAnnotation).asScala.collect {
      case annotation: Resource => annotation
    }.map { annotation =>
      IRI.create(annotation.getURI) -> model.listObjectsOfProperty(annotation, dcSource).asScala.collect {
        case resource: Resource => Option(resource.getURI)
      }.flatten.map(IRI.create).toSet
    }.toSet
  }

  def expressionProfile(iri: IRI): Future[Seq[SourcedMinimalTerm]] = {
    val query = construct(
      t(iri, hasAnnotation.asNode, 'entity),
      t('entity, dcSource, 'source)) from "http://kb.phenoscape.org/" where(
      bgp(
        t('expression, rdfType, GeneExpression),
        t('expression, associated_with_gene, iri),
        t('expression, occurs_in, 'entity)),
      optional(bgp(t('expression, dcSource, 'source))),
      new ElementFilter(new E_NotOneOf(new ExprVar('entity), new ExprList(List[Expr](
        new NodeValueNode(owlNamedIndividual), new NodeValueNode(owlClass)).asJava))))
    for {
      annotationsData <- App.executeSPARQLConstructQuery(query)
      entitiesWithSources = processProfileResultToAnnotationsAndSources(annotationsData)
      labelledEntities <- Future.sequence(entitiesWithSources.map {
        case (entity, sources) => Term.computedLabel(entity).map(SourcedMinimalTerm(_, sources))
      })
    } yield labelledEntities.toSeq.sortBy(_.term.label.toLowerCase)
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
    select_distinct() from KBMainGraph.toString from KBClosureGraph.toString where bgp(
      t('gene, rdfsLabel, 'gene_label),
      t('expression, associated_with_gene, 'gene),
      //        t('expression, rdfType, GeneExpression), // faster without, and not necessary for current model
      t('expression, occurs_in / rdfsSubClassOf / partOfSome, entityIRI))
  }

  def taxonForGeneIRI(iri: IRI): Taxon = geneIDPrefixToTaxon.collectFirst {
    case (prefix, taxon) if iri.toString.startsWith(prefix) => taxon
  }.getOrElse(Taxon(factory.getOWLThing.getIRI, "unknown"))

  def apply(result: QuerySolution): Gene = {
    val geneURI = result.getResource("gene").getURI
    Gene(
      IRI.create(geneURI),
      result.getLiteral("gene_label").getLexicalForm,
      taxonForGeneIRI(IRI.create(geneURI)))
  }

  implicit val GenesTextMarshaller: ToEntityMarshaller[Seq[Gene]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { genes =>
    val header = "IRI\tlabel\ttaxon"
    s"$header\n${genes.map(_.toString).mkString("\n")}"
  }

  implicit val ComboGenesMarshaller: ToEntityMarshaller[Seq[Gene]] = Marshaller.oneOf(GenesTextMarshaller, JSONResultItemsMarshaller)

}

case class Gene(iri: IRI, label: String, taxon: Taxon) extends LabeledTerm with JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString.toJson, "label" -> label.toJson, "taxon" -> taxon.toJSON).toJson.asJsObject
  }

  override def toString: String = {
    s"$iri\t$label\t${taxon.label}"
  }

}