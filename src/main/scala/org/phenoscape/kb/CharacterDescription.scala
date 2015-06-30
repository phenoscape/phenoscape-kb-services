package org.phenoscape.kb

import scala.concurrent.Future
import org.phenoscape.owl.Vocab._
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
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

  def query(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Future[Seq[CharacterDescription]] = for {
    query <- App.expandWithOwlet(buildQuery(entity, taxon, publications, limit, offset))
    descriptions <- App.executeSPARQLQuery(query, CharacterDescription(_))
  } yield {
    descriptions
  }

  def queryTotal(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Future[ResultCount] = for {
    query <- App.expandWithOwlet(buildTotalQuery(entity, taxon, publications))
    result <- App.executeSPARQLQuery(query)
  } yield {
    ResultCount(result)
  }

  def queryVariationProfile(taxa: Seq[IRI], limit: Int = 20, offset: Int = 0): Future[Seq[CharacterDescriptionAnnotation]] =
    App.executeSPARQLQuery(buildVariationProfileQuery(taxa, limit, offset), CharacterDescriptionAnnotation(_))

  def queryVariationProfileTotal(taxa: Seq[IRI]): Future[Int] =
    App.executeSPARQLQuery(buildVariationProfileTotalQuery(taxa)).map(ResultCount.count)

  def withIRI(iri: IRI): Future[Option[CharacterDescription]] =
    App.executeSPARQLQuery(buildCharacterDescriptionQuery(iri), fromQuerySolution(iri)).map(_.headOption)

  def annotatedCharacterDescriptionWithAnnotation(iri: IRI): Future[Option[AnnotatedCharacterDescription]] = {
    val query = select_distinct('state, 'description, 'matrix, 'matrix_label, 'phenotype) where (
      bgp(
        t('state, describes_phenotype, 'phenotype),
        t('state, dcDescription, 'description),
        t('matrix, has_character / may_have_state_value, 'state),
        t('matrix, rdfsLabel, 'matrix_label)))
    val result = App.executeSPARQLQuery(query, result => {
      Term.computedLabel(IRI.create(result.getResource("phenotype").getURI)).map { phenotype =>
        AnnotatedCharacterDescription(
          CharacterDescription(
            IRI.create(result.getResource("state").getURI),
            result.getLiteral("description").getLexicalForm,
            CharacterMatrix(
              IRI.create(result.getResource("matrix").getURI),
              result.getLiteral("matrix_label").getLexicalForm)),
          phenotype)
      }
    })
    result.flatMap(Future.sequence(_)).map(_.headOption)
  }

  def buildBasicQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Query = {
    val entityPatterns = if (entity == owlThing) Nil else
      t('phenotype, ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
    val taxonPatterns = if (taxon == owlThing) Nil else
      t('taxon, exhibits_state, 'state) :: t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil
    val filters = if (publications.isEmpty) Nil else
      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, dcDescription, 'state_desc) ::
          t('state, describes_phenotype, 'phenotype) ::
          t('matrix, has_character / may_have_state_value, 'state) ::
          t('matrix, rdfsLabel, 'matrix_label) ::
          entityPatterns ++
          taxonPatterns: _*) ::
        filters: _*)
  }

  def buildQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil, limit: Int = 20, offset: Int = 0): Query = {
    val query = buildBasicQuery(entity, taxon, publications)
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

  def buildTotalQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Query = {
    val query = buildBasicQuery(entity, taxon, publications)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    query
  }

  def buildVariationProfileTotalQuery(taxa: Seq[IRI]): Query = {
    val query = buildBasicVariationProfileQuery(taxa)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("phenotype"))))
    query
  }

  def buildVariationProfileQuery(taxa: Seq[IRI], limit: Int = 20, offset: Int = 0): Query = {
    val query = buildBasicVariationProfileQuery(taxa)
    query.addResultVar('state)
    query.addResultVar('state_desc)
    query.addResultVar('phenotype)
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('state_desc)
    query.addOrderBy('state)
    query.addOrderBy('phenotype)
    query
  }

  def buildBasicVariationProfileQuery(taxa: Seq[IRI]): Query = {
    val filters = if (taxa.isEmpty) Nil else
      new ElementFilter(new E_OneOf(new ExprVar('taxon), new ExprList(taxa.map(new NodeValueNode(_)).toList))) :: Nil
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, has_phenotypic_profile, 'profile),
        t('profile, rdfType, 'phenotype),
        t('state, dcDescription, 'state_desc),
        t('state, describes_phenotype, 'phenotype)) ::
        filters: _*)
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

  def fromQuerySolution(iri: IRI)(result: QuerySolution): CharacterDescription = CharacterDescription(iri,
    result.getLiteral("state_desc").getLexicalForm,
    CharacterMatrix(
      IRI.create(result.getResource("matrix").getURI),
      result.getLiteral("matrix_label").getLexicalForm))

}

case class CharacterDescription(iri: IRI, description: String, matrix: CharacterMatrix) extends JSONResultItem {

  def toJSON: JsObject = Map(
    "@id" -> iri.toString.toJson,
    "description" -> description.toJson,
    "matrix" -> matrix.toJSON).toJson.asJsObject

}

case class CharacterDescriptionAnnotation(iri: IRI, description: String, annotation: IRI) extends JSONResultItem {

  def toJSON: JsObject = Map(
    "@id" -> iri.toString.toJson,
    "description" -> description.toJson,
    "annotation" -> annotation.toString.toJson).toJson.asJsObject

}

object CharacterDescriptionAnnotation {

  def apply(result: QuerySolution): CharacterDescriptionAnnotation = CharacterDescriptionAnnotation(
    IRI.create(result.getResource("state").getURI),
    result.getLiteral("state_desc").getLexicalForm,
    IRI.create(result.getResource("phenotype").getURI))

}

case class AnnotatedCharacterDescription(characterDescription: CharacterDescription, phenotype: MinimalTerm) extends JSONResultItem {

  def toJSON: JsObject = (characterDescription.toJSON.fields ++ Map("phenotype" -> phenotype.toJSON)).toJson.asJsObject

}

case class CharacterMatrix(iri: IRI, label: String) {

  def toJSON: JsValue = {
    Map("@id" -> iri.toString, "label" -> label).toJson
  }

}