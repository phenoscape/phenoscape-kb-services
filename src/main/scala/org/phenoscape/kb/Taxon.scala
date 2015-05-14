package org.phenoscape.kb

import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.language.postfixOps
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
import com.hp.hpl.jena.sparql.expr.E_Coalesce
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString
import com.hp.hpl.jena.sparql.syntax.ElementBind

object Taxon {

  val phylopic = ObjectProperty("http://purl.org/phenoscape/phylopics.owl#phylopic")
  val group_label = ObjectProperty("http://purl.org/phenoscape/phylopics.owl#group_label")
  val is_extinct = ObjectProperty("http://purl.obolibrary.org/obo/vto#is_extinct")

  def withIRI(iri: IRI): Future[Option[TaxonInfo]] =
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

  def variationProfileFor(taxon: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[IRI]] =
    App.executeSPARQLQuery(buildVariationProfileQuery(taxon, limit, offset), result => IRI.create(result.getResource("phenotype").getURI))

  def variationProfileTotalFor(taxon: IRI): Future[Int] =
    App.executeSPARQLQuery(buildVariationProfileTotalQuery(taxon)).map(ResultCount.count)

  def commonGroupFor(taxon: IRI): Future[Option[CommonGroup]] = {
    App.executeSPARQLQuery(buildPhylopicQuery(taxon), CommonGroup(_)).map(_.headOption)
  }

  def directPhenotypesFor(taxon: IRI, limit: Int = 20, offset: Int = 0): Future[Seq[IRI]] = {
    val query = buildPhenotypesQuery(taxon)
    query.addResultVar('phenotype)
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('phenotype)
    App.executeSPARQLQuery(query, result => IRI.create(result.getResource("phenotype").getURI))
  }

  def directPhenotypesTotalFor(taxon: IRI): Future[Int] = {
    val query = buildPhenotypesQuery(taxon)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("phenotype"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  private def buildPhenotypesQuery(taxon: IRI): Query =
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, exhibits_state / describes_phenotype, 'phenotype)))

  def phyloPicAcknowledgments: Future[Seq[IRI]] = {
    val query = select_distinct('pic) where (bgp(t('subject, phylopic, 'pic)))
    App.executeSPARQLQuery(query, result => IRI.create(result.getResource("pic").getURI))
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
    query
  }

  def buildTaxonQuery(iri: IRI): Query =
    select('label, 'is_extinct) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'label),
        t(iri, rdfsIsDefinedBy, VTO)),
        optional(
          bgp(
            t(iri, is_extinct, 'is_extinct))))

  def buildVariationProfileTotalQuery(taxon: IRI): Query = {
    val query = buildBasicVariationProfileQuery(taxon)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("phenotype"))))
    query
  }

  def buildVariationProfileQuery(taxon: IRI, limit: Int, offset: Int): Query = {
    val query = buildBasicVariationProfileQuery(taxon)
    query.addResultVar('phenotype)
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('phenotype)
    query
  }

  def buildBasicVariationProfileQuery(taxon: IRI): Query = {
    val hasPhenotypicProfile = ObjectProperty(has_phenotypic_profile)
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, hasPhenotypicProfile / rdfType, 'phenotype)))
  }

  def buildPhylopicQuery(taxon: IRI): Query = {
    val rdfsSubClassOf = ObjectProperty(Vocab.rdfsSubClassOf)
    val query = select('super, 'label, 'picOpt) from "http://kb.phenoscape.org/" from "http://purl.org/phenoscape/phylopics.owl" where (
      bgp(
        t(taxon, rdfsSubClassOf*, 'super),
        t('super, group_label, 'label),
        t('super, rdfsSubClassOf*, 'ancestor)),
        optional(
          bgp(
            t('super, phylopic, 'pic))),
          new ElementBind('picOpt, new E_Coalesce(new ExprList(Seq(new ExprVar("pic"), new NodeValueString("")))))) order_by desc('level) limit 1
    query.addGroupBy('super)
    query.addGroupBy('label)
    query.addGroupBy('picOpt)
    query.getProject.add(Var.alloc("level"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("ancestor"))))
    query
  }

  def apply(result: QuerySolution): Taxon = Taxon(
    IRI.create(result.getResource("taxon").getURI),
    result.getLiteral("taxon_label").getLexicalForm)

  def fromIRIQuery(iri: IRI)(result: QuerySolution): TaxonInfo = TaxonInfo(
    iri,
    result.getLiteral("label").getLexicalForm,
    Option(result.getLiteral("is_extinct")).map(_.getBoolean).getOrElse(false))

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
    val escapedLabel = s"'${parentLabel.replaceAllLiterally("'", "\"")}'"
    val parentCount = model.listObjectsOfProperty(parent, RDFS.subClassOf).size
    if (parentCount > 1) println(s"WARNING: $parentCount parents for $parent")
    val children = model.listResourcesWithProperty(RDFS.subClassOf, parent).toSeq
    val childList = children.map(newickFor(_, model)).mkString(", ")
    val subtree = if (children.isEmpty) "" else s"($childList)"
    s"$subtree$escapedLabel"
  }

}

case class CommonGroup(label: String, phylopic: Option[IRI]) extends JSONResultItem {

  def toJSON: JsObject = {
    val pic = phylopic.map(iri => Map("phylopic" -> iri.toString)).getOrElse(Map.empty)
    (Map("label" -> label) ++ pic).toJson.asJsObject
  }

}

object CommonGroup {

  def apply(result: QuerySolution): CommonGroup = {
    CommonGroup(result.getLiteral("label").getLexicalForm,
      {
        val picOpt = result.get("picOpt")
        if (picOpt.isURIResource) Some(IRI.create(picOpt.asResource.getURI)) else None
      })
  }

}

case class Taxon(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString.toJson, "label" -> label.toJson).toJson.asJsObject
  }

}

case class TaxonInfo(iri: IRI, label: String, extinct: Boolean) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString.toJson, "label" -> label.toJson, "extinct" -> extinct.toJson).toJson.asJsObject
  }

}

