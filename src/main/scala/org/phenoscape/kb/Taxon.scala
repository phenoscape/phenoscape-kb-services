package org.phenoscape.kb

import scala.concurrent.Future
import org.phenoscape.kb.Main.system.dispatcher
import org.semanticweb.owlapi.model.IRI
import spray.json.DefaultJsonProtocol._
import spray.json._
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.scowl.OWL._
import org.phenoscape.owl.Vocab._
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

object Taxon {

  def query(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Future[Seq[Taxon]] = for {
    query <- App.expandWithOwlet(buildQuery(entity, taxon, publications, limit, offset))
    descriptions <- App.executeSPARQLQuery(query, Taxon(_))
  } yield {
    descriptions
  }

  def queryTotal(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Future[ResultCount] = for {
    query <- App.expandWithOwlet(buildTotalQuery(entity, taxon, publications, limit, offset))
    result <- App.executeSPARQLQuery(query)
  } yield {
    ResultCount(result)
  }

  def buildQuery(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Query = {
    //val query = TaxonEQAnnotation.buildQuery(entity, taxon, publications)

    val entityPatterns = if (entity == OWLThing) Nil else
      t('phenotype, ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val filters = if (publications.isEmpty) Nil else
      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, describes_phenotype, 'phenotype) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('taxon, exhibits_state / describes_phenotype, 'state) ::
          t('taxon, rdfsLabel, 'taxon_label) ::
          entityPatterns: _*) ::
        filters: _*)

    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    query.setOffset(offset)
    query.setLimit(limit)
    query.addOrderBy('taxon_label)
    query.addOrderBy('taxon)
    query
  }

  def buildTotalQuery(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Query = {
    //val query = TaxonEQAnnotation.buildQuery(entity, taxon, publications)
    val entityPatterns = if (entity == OWLThing) Nil else
      t('phenotype, ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val filters = if (publications.isEmpty) Nil else
      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, describes_phenotype, 'phenotype) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('taxon, exhibits_state / describes_phenotype, 'state) ::
          t('taxon, rdfsLabel, 'taxon_label) ::
          entityPatterns: _*) ::
        filters: _*)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    query
  }

  def apply(result: QuerySolution): Taxon = Taxon(
    IRI.create(result.getResource("taxon").getURI),
    result.getLiteral("taxon_label").getLexicalForm)

  implicit val CharacterDescriptionsMarshaller = Marshaller.delegate[Seq[CharacterDescription], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

}

case class Taxon(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = {
    Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject
  }

}