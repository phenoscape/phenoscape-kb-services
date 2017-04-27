package org.phenoscape.kb

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.implicitConversions

import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_NotOneOf
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.syntax.ElementFilter
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLObjectProperty

import spray.http._
import spray.httpx._
import spray.httpx.marshalling._
import spray.json._
import spray.json.DefaultJsonProtocol._

object Gene {

  private val factory = OWLManager.getOWLDataFactory

  //FIXME this is a temporary hack until genes are directly associated with taxa in the KB
  val geneIDPrefixToTaxon = Map(
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
    val query = select_distinct('gene, 'gene_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('gene_label, BDSearch, NodeFactory.createLiteral(searchText)),
        t('gene_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('gene_label, BDRank, 'rank),
        t('gene, rdfsLabel, 'gene_label),
        t('gene, rdfType, Vocab.Gene)))
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
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('annotation, rdfType, AnnotatedPhenotype) ::
          t('annotation, associated_with_gene, 'gene) ::
          t('gene, rdfsLabel, 'gene_label) ::
          taxonPatterns ++
          entityPatterns: _*))
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
    select('gene_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'gene_label),
        t(iri, rdfType, Vocab.Gene)))

  def fromIRIQuery(iri: IRI)(result: QuerySolution): Gene = Gene(
    iri,
    result.getLiteral("gene_label").getLexicalForm,
    taxonForGeneIRI(iri))

  def affectingPhenotypeOfEntity(entity: IRI, quality: Option[IRI], includeParts: Boolean, includeHomologs: Boolean, limit: Int, offset: Int): Future[Seq[Gene]] = {
    val query = buildGeneForPhenotypeQuery(entity, quality, includeParts, includeHomologs)
    query.addResultVar('gene)
    query.addResultVar('gene_label)
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('gene_label)
    query.addOrderBy('gene)
    for {
      expandedQuery <- App.expandWithOwlet(query)
      genes <- App.executeSPARQLQuery(expandedQuery, Gene(_))
    } yield genes
  }

  def affectingPhenotypeOfEntityTotal(entity: IRI, quality: Option[IRI], includeParts: Boolean, includeHomologs: Boolean): Future[Int] = {
    val query = buildGeneForPhenotypeQuery(entity, quality, includeParts, includeHomologs)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("gene"))))
    for {
      expandedQuery <- App.expandWithOwlet(query)
      total <- App.executeSPARQLQuery(expandedQuery).map(ResultCount.count)
    } yield total
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
      t('annotation, dcSource, 'source)) from "http://kb.phenoscape.org/" where (
        bgp(
          t('pheno_instance, rdfType, AnnotatedPhenotype),
          t('pheno_instance, associated_with_gene, iri),
          t('pheno_instance, rdfType, 'annotation)),
          optional(bgp(t('pheno_instance, dcSource, 'source))),
          new ElementFilter(new E_NotOneOf(new ExprVar('annotation), new ExprList(List(
            new NodeValueNode(AnnotatedPhenotype),
            new NodeValueNode(owlNamedIndividual))))))
    for {
      annotationsData <- App.executeSPARQLConstructQuery(query)
      phenotypesWithSources = processProfileResultToAnnotationsAndSources(annotationsData)
      labelledPhenotypes <- Future.sequence(phenotypesWithSources.map {
        case (phenotype, sources) => Term.computedLabel(phenotype).map(SourcedMinimalTerm(_, sources))
      })
    } yield labelledPhenotypes.toSeq.sortBy(_.term.label)
  }

  private val hasAnnotation = ResourceFactory.createProperty("http://example.org/hasAnnotation")
  private implicit def objectPropertyToJenaProperty(prop: OWLObjectProperty): Property = {
    ResourceFactory.createProperty(prop.getIRI.toString)
  }

  private def processProfileResultToAnnotationsAndSources(model: Model): Set[(IRI, Set[IRI])] = {
    model.listObjectsOfProperty(hasAnnotation).collect {
      case annotation: Resource => annotation
    }.map { annotation =>
      IRI.create(annotation.getURI) -> model.listObjectsOfProperty(annotation, dcSource).collect {
        case resource: Resource => Option(resource.getURI)
      }.flatten.map(IRI.create).toSet
    }.toSet
  }

  def expressionProfile(iri: IRI): Future[Seq[SourcedMinimalTerm]] = {
    val query = construct(
      t(iri, hasAnnotation.asNode, 'entity),
      t('entity, dcSource, 'source)) from "http://kb.phenoscape.org/" where (
        bgp(
          t('expression, rdfType, GeneExpression),
          t('expression, associated_with_gene, iri),
          t('expression, occurs_in, 'entity_instance),
          t('entity_instance, rdfType, 'entity)),
          optional(bgp(t('expression, dcSource, 'source))),
          new ElementFilter(new E_NotOneOf(new ExprVar('entity), new ExprList(List(
            new NodeValueNode(owlNamedIndividual))))))
    for {
      annotationsData <- App.executeSPARQLConstructQuery(query)
      entitiesWithSources = processProfileResultToAnnotationsAndSources(annotationsData)
      labelledEntities <- Future.sequence(entitiesWithSources.map {
        case (entity, sources) => Term.computedLabel(entity).map(SourcedMinimalTerm(_, sources))
      })
    } yield labelledEntities.toSeq.sortBy(_.term.label)
  }

  case class Phenotype(iri: IRI)

  private def formatResult(result: QuerySolution): String = {
    val gene = result.getResource("gene").getURI
    val geneLabel = result.getLiteral("gene_label").getLexicalForm
    val taxon = result.getLiteral("taxon_label").getLexicalForm
    val source = Option(result.getResource("source")).map(_.getURI).getOrElse("")
    s"$gene\t$geneLabel\t$taxon\t$source"
  }

  private def buildGeneForPhenotypeQuery(entityIRI: IRI, quality: Option[IRI], includeParts: Boolean, includeHomologs: Boolean): Query = {
    val hasPhenotypicProfile = ObjectProperty(has_phenotypic_profile)
    val entityClass = Class(entityIRI)
    val homologousEntityClass = if (includeHomologs) (entityClass or (homologous_to some entityClass)) else entityClass
    val entityExpression = if (includeParts) (part_of some homologousEntityClass) else homologousEntityClass
    val phenotypeExpression = quality match {
      case Some(qualityTerm) => (has_part some Class(qualityTerm)) and (phenotype_of some entityExpression)
      case None              => (phenotype_of some entityExpression) or (has_part some (towards value Individual(entityIRI)))
    }
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        App.BigdataAnalyticQuery,
        t('gene, rdfsLabel, 'gene_label),
        t('gene, rdfType, Vocab.Gene),
        t('gene, hasPhenotypicProfile / rdfType, 'phenotype),
        t('phenotype, rdfsSubClassOf, phenotypeExpression.asOMN)))
    //TODO add part_of instance graph so could say (towards some (part_of value Individual(entityIRI)))???
  }

  private def buildGeneExpressedInEntityQuery(entityIRI: IRI): Query = {
    val partOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)
    select_distinct() from KBMainGraph from KBClosureGraph where (
      bgp(
        App.BigdataAnalyticQuery,
        t('gene, rdfsLabel, 'gene_label),
        t('expression, associated_with_gene, 'gene),
        t('expression, rdfType, GeneExpression),
        t('expression, occurs_in / ObjectProperty(rdfType) / rdfsSubClassOf / partOfSome, entityIRI)))
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

  val GenesTextMarshaller = Marshaller.delegate[Seq[Gene], String](MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`) { genes =>
    val header = "IRI\tlabel\ttaxon"
    s"$header\n${genes.map(_.toString).mkString("\n")}"
  }

  implicit val ComboGenesMarshaller = ToResponseMarshaller.oneOf(MediaTypes.`text/plain`, MediaTypes.`text/tab-separated-values`, MediaTypes.`application/json`)(GenesTextMarshaller, JSONResultItemsMarshaller)

}

case class Gene(iri: IRI, label: String, taxon: Taxon) extends LabeledTerm with JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString.toJson, "label" -> label.toJson, "taxon" -> taxon.toJSON).toJson.asJsObject
  }

  override def toString(): String = {
    s"$iri\t$label\t${taxon.label}"
  }

}