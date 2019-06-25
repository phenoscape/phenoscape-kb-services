package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.{Query, QuerySolution}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.expr.{E_OneOf, Expr, ExprList, ExprVar}
import org.apache.jena.sparql.syntax.ElementFilter
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.TaxonEQAnnotation.{ps_entity_term, ps_related_entity_term}
import org.phenoscape.kb.JSONResultItem.JSONResultItemsMarshaller
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.model.{IRI, OWLClassExpression}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.Future

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
    val query = select_distinct('state, 'description, 'matrix, 'matrix_label) where bgp(
      t('state, describes_phenotype, iri),
      t('state, dcDescription, 'description),
      t('matrix, has_character / may_have_state_value, 'state),
      t('matrix, rdfsLabel, 'matrix_label))
    val result = App.executeSPARQLQuery(query, result => {
      Term.computedLabel(iri).map { phenotype =>
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
      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toBuffer[Expr].asJava))) :: Nil
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
      new ElementFilter(new E_OneOf(new ExprVar('taxon), new ExprList(taxa.map(new NodeValueNode(_)).toBuffer[Expr].asJava))) :: Nil
    select_distinct() from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, has_phenotypic_profile, 'profile),
        t('profile, rdfType, 'phenotype),
        t('state, dcDescription, 'state_desc),
        t('state, describes_phenotype, 'phenotype)) ::
        filters: _*)
  }

  def buildSearchQuery(text: String, limit: Int): Query = {
    val query = select_distinct('state, 'state_desc, 'matrix, 'matrix_label) from "http://kb.phenoscape.org/" where bgp(
      t('state_desc, BDSearch, NodeFactory.createLiteral(text)),
      t('state_desc, BDMatchAllTerms, NodeFactory.createLiteral("true")),
      t('state_desc, BDRank, 'rank),
      t('state, dcDescription, 'state_desc),
      t('state, rdfType, StandardState),
      t('character, may_have_state_value, 'state),
      t('matrix, has_character, 'character),
      t('matrix, rdfsLabel, 'matrix_label)) order_by asc('rank)
    query.setLimit(limit)
    query
  }

  def buildCharacterDescriptionQuery(iri: IRI): Query = {
    select_distinct('state_desc, 'matrix, 'matrix_label) from "http://kb.phenoscape.org/" where bgp(
      t(iri, dcDescription, 'state_desc),
      t('character, may_have_state_value, iri),
      t('matrix, has_character, 'character),
      t('matrix, rdfsLabel, 'matrix_label))
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

  def eqAnnotationsForPhenotype(iri: IRI): Future[Seq[MinimalTerm]] = {
    val query = select_distinct('eq) from "http://kb.phenoscape.org/" where bgp(
      t(iri, rdfsSubClassOf, 'eq))
    for {
      eqs <- App.executeSPARQLQuery(query, result => IRI.create(result.getResource("eq").getURI))
      labeledEQs <- Future.sequence(eqs.map(Term.computedLabel))
    } yield labeledEQs.groupBy(_.label).map {
      case (label, terms) => //FIXME this groupBy is to work around extra inferred identical EQs; need to fix in Phenex translation
        terms.head
    }.toSeq
  }

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

  override def toString: String = {
    s"${phenotype.iri}\t${phenotype.label}\t${characterDescription.iri}\t${characterDescription.description}\t${characterDescription.matrix.iri}\t${characterDescription.matrix.label}"
  }

}

object AnnotatedCharacterDescription { //FIXME

  def fromQuerySolution(result: QuerySolution): Future[AnnotatedCharacterDescription] = {
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
  }

  implicit val AnnotatedCharacterDescriptionsTextMarshaller: ToEntityMarshaller[Seq[AnnotatedCharacterDescription]] = Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { annotations =>
    val header = "phenotype IRI\tphenotype\tcharacter description IRI\tcharacter description\tstudy IRI\tstudy"
    s"$header\n${annotations.map(_.toString).mkString("\n")}"
  }

  implicit val ComboAnnotatedCharacterDescriptionsMarshaller: ToEntityMarshaller[Seq[AnnotatedCharacterDescription]] = Marshaller.oneOf(AnnotatedCharacterDescriptionsTextMarshaller, JSONResultItemsMarshaller)

}

case class CharacterMatrix(iri: IRI, label: String) {

  def toJSON: JsValue = {
    Map("@id" -> iri.toString, "label" -> label).toJson
  }

}