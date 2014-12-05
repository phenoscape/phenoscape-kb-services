package org.phenoscape.kb

import scala.concurrent.Future
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_LABEL
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.expr.E_IsIRI
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.vocabulary.OWL2
import spray.http._
import spray.httpx._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._
import spray.json._
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.E_OneOf
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.Main.system.dispatcher
import org.semanticweb.owlapi.model.OWLClassExpression
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import com.hp.hpl.jena.graph.Node
import org.semanticweb.owlapi.apibinding.OWLManager
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct
import com.hp.hpl.jena.sparql.core.Var
import TaxonEQAnnotation.ps_entity_term
import TaxonEQAnnotation.ps_quality_term
import TaxonEQAnnotation.ps_related_entity_term

object CharacterDescription {

  def search(text: String, limit: Int): Future[Seq[CharacterDescription]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, limit), CharacterDescription(_))
  }

  def query(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Future[Seq[CharacterDescription]] = for {
    query <- App.expandWithOwlet(buildQuery(entity, taxon, publications, limit, offset))
    descriptions <- App.executeSPARQLQuery(query, CharacterDescription(_))
  } yield {
    descriptions
  }

  def queryTotal(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Future[ResultCount] = for {
    query <- App.expandWithOwlet(buildTotalQuery(entity, taxon, publications, limit, offset))
    result <- App.executeSPARQLQuery(query)
  } yield {
    ResultCount(result)
  }

  def withIRI(iri: IRI): Future[Option[CharacterDescription]] = {
    App.executeSPARQLQuery(buildCharacterDescriptionQuery(iri), fromQuerySolution(_)(iri)).map(_.headOption)
  }

  def buildQuery(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Query = {
    //val query = TaxonEQAnnotation.buildQuery(entity, taxon, publications)

    val entityPatterns = if (entity == OWLThing) Nil else
      t('phenotype, TaxonEQAnnotation.ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val filters = if (publications.isEmpty) Nil else
      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, dcDescription, 'state_desc) ::
          t('state, describes_phenotype, 'phenotype) ::
          t('phenotype, ps_entity_term, 'annotated_entity) ::
          t('phenotype, ps_quality_term, 'annotated_quality) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('matrix, rdfsLabel, 'matrix_label) ::
          entityPatterns: _*) ::
        optional(bgp(t('phenotype, ps_related_entity_term, 'annotated_related_entity))) ::
        filters: _*)

    query.addResultVar('state)
    query.addResultVar('state_desc)
    query.addResultVar('matrix)
    query.addResultVar('matrix_label)
    query.setOffset(offset)
    query.setLimit(limit)
    query.addOrderBy('state_desc)
    query.addOrderBy('state)
    query
  }

  def buildTotalQuery(entity: OWLClassExpression = OWLThing, taxon: OWLClassExpression = OWLThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Query = {
    //val query = TaxonEQAnnotation.buildQuery(entity, taxon, publications)
    val entityPatterns = if (entity == OWLThing) Nil else
      t('phenotype, TaxonEQAnnotation.ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val filters = if (publications.isEmpty) Nil else
      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil
    val query = select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, dcDescription, 'state_desc) ::
          t('state, describes_phenotype, 'phenotype) ::
          t('phenotype, ps_entity_term, 'annotated_entity) ::
          t('phenotype, ps_quality_term, 'annotated_quality) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('matrix, rdfsLabel, 'matrix_label) ::
          entityPatterns: _*) ::
        optional(bgp(t('phenotype, ps_related_entity_term, 'annotated_related_entity))) ::
        filters: _*)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    query
  }

  def buildSearchQuery(text: String, limit: Int): Query = {
    val query = select_distinct('state, 'state_desc, 'matrix, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('state_desc, BDSearch, NodeFactory.createLiteral(text)),
        t('state_desc, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('state_desc, BDRank, 'rank),
        t('state, dcDescription, 'state_desc),
        t('state, rdfType, StandardState),
        t('character, may_have_state_value, 'state),
        t('matrix, has_character, 'character),
        t('matrix, rdfsLabel, 'matrix_label))) order_by asc('rank)
    query.setLimit(limit)
    query
  }

  def buildCharacterDescriptionQuery(iri: IRI): Query = {
    select_distinct('state_desc, 'matrix, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, dcDescription, 'state_desc),
        t('character, may_have_state_value, iri),
        t('matrix, has_character, 'character),
        t('matrix, rdfsLabel, 'matrix_label)))
  }

  def apply(result: QuerySolution): CharacterDescription = CharacterDescription(
    IRI.create(result.getResource("state").getURI),
    result.getLiteral("state_desc").getLexicalForm,
    CharacterMatrix(
      IRI.create(result.getResource("matrix").getURI),
      result.getLiteral("matrix_label").getLexicalForm))

  def fromQuerySolution(result: QuerySolution)(iri: IRI): CharacterDescription = CharacterDescription(iri,
    result.getLiteral("state_desc").getLexicalForm,
    CharacterMatrix(
      IRI.create(result.getResource("matrix").getURI),
      result.getLiteral("matrix_label").getLexicalForm))

  implicit val CharacterDescriptionsMarshaller = Marshaller.delegate[Seq[CharacterDescription], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

}

case class CharacterDescription(iri: IRI, description: String, matrix: CharacterMatrix) {

  def toJSON: JsValue = Map(
    "@id" -> iri.toString.toJson,
    "description" -> description.toJson,
    "matrix" -> matrix.toJSON).toJson

}

case class CharacterMatrix(iri: IRI, label: String) {

  def toJSON: JsValue = {
    Map("@id" -> iri.toString, "label" -> label).toJson
  }

}