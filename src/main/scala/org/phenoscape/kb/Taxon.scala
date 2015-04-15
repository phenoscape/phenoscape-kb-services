package org.phenoscape.kb

import scala.concurrent.Future
import scala.collection.JavaConversions._
import org.phenoscape.kb.Main.system.dispatcher
import org.semanticweb.owlapi.model.IRI
import spray.json._
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._
import org.phenoscape.owl.Vocab
import org.phenoscape.owl.Vocab._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.scowl.OWL._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.OWLClassExpression
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.E_OneOf
import com.hp.hpl.jena.sparql.expr.E_IsIRI
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct
import com.hp.hpl.jena.sparql.core.Var
import TaxonEQAnnotation.ps_entity_term
import TaxonEQAnnotation.ps_quality_term
import TaxonEQAnnotation.ps_related_entity_term
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Resource

object Taxon {

  def withIRI(iri: IRI): Future[Option[Taxon]] =
    App.executeSPARQLQuery(buildTaxonQuery(iri), Taxon.fromIRIQuery(iri)).map(_.headOption)

  def query(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Future[Seq[Taxon]] = for {
    query <- App.expandWithOwlet(buildQuery(entity, taxon, publications, limit, offset))
    descriptions <- App.executeSPARQLQuery(query, Taxon(_))
  } yield {
    descriptions
  }

  def queryTotal(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Future[ResultCount] = for {
    query <- App.expandWithOwlet(buildTotalQuery(entity, taxon, publications))
    result <- App.executeSPARQLQuery(query)
  } yield {
    ResultCount(result)
  }

  private def buildBasicQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Query = {
    val entityPatterns = if (entity == owlThing) Nil else
      t('phenotype, ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val (publicationFilters, publicationPatterns) = if (publications.isEmpty) (Nil, Nil) else
      (new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil,
        t('matrix, has_character / may_have_state_value, 'state) :: Nil)
    val taxonPatterns = if (taxon == owlThing) Nil else
      t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, describes_phenotype, 'phenotype) ::
          t('taxon, exhibits_state, 'state) ::
          t('taxon, rdfsLabel, 'taxon_label) ::
          entityPatterns ++
          publicationPatterns ++
          taxonPatterns: _*) ::
        publicationFilters: _*)
  }

  def buildQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Query = {
    val query = buildBasicQuery(entity, taxon, publications)
    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    query.setOffset(offset)
    if (limit > 0) query.setLimit(limit)
    query.addOrderBy('taxon_label)
    query.addOrderBy('taxon)
    query
  }

  def buildTotalQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Query = {
    val query = buildBasicQuery(entity, taxon, publications)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    query
  }

  def buildTaxonQuery(iri: IRI): Query =
    select('label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'label),
        t(iri, rdfsIsDefinedBy, VTO)))

  def apply(result: QuerySolution): Taxon = Taxon(
    IRI.create(result.getResource("taxon").getURI),
    result.getLiteral("taxon_label").getLexicalForm)

  def fromIRIQuery(iri: IRI)(result: QuerySolution): Taxon = Taxon(iri, result.getLiteral("label").getLexicalForm)

  def newickTreeWithRoot(iri: IRI): Future[String] = {
    val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
    val query = construct(
      t('child, rdfsSubClassOf, 'parent),
      t('child, rdfsLabel, 'label)) from "http://kb.phenoscape.org/" where (
        bgp(
          t('parent, rdfsSubClassOf*, iri),
          t('child, rdfsSubClassOf, 'parent),
          t('child, rdfsLabel, 'label),
          t('child, rdfsIsDefinedBy, VTO),
          t('parent, rdfsIsDefinedBy, VTO)))
    for {
      model <- App.executeSPARQLConstructQuery(query)
      taxon <- Term.computedLabel(iri)
    } yield {
      val taxonResource = ResourceFactory.createResource(iri.toString)
      model.add(taxonResource, RDFS.label, taxon.label)
      newickFor(taxonResource, model)
    }
  }

  private def newickFor(parent: Resource, model: Model): String = {
    val reserved = Set(';', ',', ':', '(', ')', ' ', '"')
    val parentLabel = model.getProperty(parent, RDFS.label).getLiteral.getLexicalForm
    //val escapedLabel = if (parentLabel.exists(reserved)) s"'$parentLabel'" else parentLabel
    val escapedLabel = s"'$parentLabel'"
    val parentCount = model.listObjectsOfProperty(parent, RDFS.subClassOf).size
    if (parentCount > 1) println(s"WARNING: $parentCount parents for $parent")
    val children = model.listResourcesWithProperty(RDFS.subClassOf, parent).toSeq
    val childList = children.map(newickFor(_, model)).mkString(", ")
    val subtree = if (children.isEmpty) "" else s"($childList)"
    s"$subtree$escapedLabel"
  }

}

case class Taxon(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject
  }

}