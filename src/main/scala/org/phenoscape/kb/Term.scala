package org.phenoscape.kb

import java.util.regex.Pattern

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.postfixOps

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_IsIRI
import org.apache.jena.sparql.expr.E_NotExists
import org.apache.jena.sparql.expr.E_OneOf
import org.apache.jena.sparql.expr.ExprList
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.aggregate.AggMin
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.path.P_Link
import org.apache.jena.sparql.path.P_OneOrMore1
import org.apache.jena.sparql.syntax.Element
import org.apache.jena.sparql.syntax.ElementFilter
import org.apache.jena.sparql.syntax.ElementGroup
import org.apache.jena.sparql.syntax.ElementUnion
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.kb.ingest.util.ExpressionUtil
import org.phenoscape.kb.util.SPARQLEntityChecker
import org.phenoscape.owl.Vocab._
import org.phenoscape.owl.util.ExpressionsUtil
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.manchestersyntax.parser.ManchesterOWLSyntaxClassExpressionParser
import org.semanticweb.owlapi.manchestersyntax.renderer.ManchesterOWLSyntaxOWLObjectRendererImpl
import org.semanticweb.owlapi.manchestersyntax.renderer.ParserException
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLEntity
import org.semanticweb.owlapi.model.OWLObject
import org.semanticweb.owlapi.util.ShortFormProvider

import Main.system.dispatcher
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import scalaz._
import spray.json._
import spray.json.DefaultJsonProtocol._

object Term {

  private val factory = OWLManager.getOWLDataFactory

  def search(text: String, termType: IRI, property: IRI): Future[Seq[MatchedTerm[MinimalTerm]]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, termType, property), Term.fromMinimalQuerySolution).map(orderBySearchedText(_, text))
  }

  def searchOntologyTerms(text: String, definedBy: IRI, limit: Int): Future[Seq[MatchedTerm[MinimalTerm]]] = {
    App.executeSPARQLQuery(buildOntologyTermQuery(text, definedBy, limit), Term.fromMinimalQuerySolution).map(orderBySearchedText(_, text))
  }

  def label(iri: IRI): Future[Option[MinimalTerm]] = {
    def convert(result: QuerySolution): MinimalTerm = MinimalTerm(iri, result.getLiteral("term_label").getLexicalForm)
    App.executeSPARQLQuery(buildLabelQuery(iri), convert).map(_.headOption)
  }

  def labels(iris: IRI*): Future[Seq[MinimalTerm]] = {
    App.executeSPARQLQuery(buildLabelsQuery(iris: _*), Term.fromMinimalQuerySolution)
  }

  def computedLabel(iri: IRI): Future[MinimalTerm] = (for {
    labelOpt <- label(iri)
  } yield {
    labelOpt.map(Future.successful).getOrElse {
      computeLabelForAnonymousTerm(iri)
    }
  }).flatMap(identity)

  def stateLabelForPhenotype(phenotype: IRI): Future[Option[MinimalTerm]] = {
    val query = select_distinct('state, 'state_desc) from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, dcDescription, 'state_desc),
        t('state, describes_phenotype, phenotype)))
    for {
      res <- App.executeSPARQLQuery(query, result => MinimalTerm(phenotype, result.getLiteral("state_desc").getLexicalForm))
    } yield res.headOption
  }

  def computeLabelForAnonymousTerm(iri: IRI): Future[MinimalTerm] = iri.toString match {
    case expression if expression.startsWith(ExpressionUtil.namedExpressionPrefix) || expression.startsWith(ExpressionUtil.namedSubClassPrefix) =>
      labelForNamedExpression(iri)
    case negation if negation.startsWith("http://phenoscape.org/not/") =>
      computedLabel(IRI.create(negation.replaceFirst(Pattern.quote("http://phenoscape.org/not/"), ""))).map { term =>
        MinimalTerm(iri, s"not ${term.label}")
      }
    case absence if absence.startsWith("http://phenoscape.org/not_has_part/") =>
      computedLabel(IRI.create(absence.replaceFirst(Pattern.quote("http://phenoscape.org/not_has_part/"), ""))).map { term =>
        MinimalTerm(iri, s"absence of ${term.label}")
      }
    case _ => Future.successful(MinimalTerm(iri, iri.toString))
  }

  def labelForNamedExpression(iri: IRI): Future[MinimalTerm] =
    ExpressionsUtil.expressionForName(Class(iri)).map { expression =>
      for {
        terms <- Future.sequence(expression.getSignature.map(term => computedLabel(term.getIRI)))
        labelMap = terms.map(term => term.iri -> term.label).toMap
      } yield {
        val renderer = createEntityRenderer(new LabelMapProvider(labelMap))
        MinimalTerm(iri, renderer(expression))
      }
    }.getOrElse(Future.successful(MinimalTerm(iri, iri.toString)))

  private def createEntityRenderer(shortFormProvider: ShortFormProvider): OWLObject => String = {
    val renderer = new ManchesterOWLSyntaxOWLObjectRendererImpl()
    renderer.setShortFormProvider(shortFormProvider)
    entity => renderer.render(entity).replaceAll("\n", " ").replaceAll("\\s+", " ")
  }

  def withIRI(iri: IRI): Future[Option[Term]] = {
    def termResult(result: QuerySolution) = (result.getLiteral("label").getLexicalForm,
      Option(result.getLiteral("definition")).map(_.getLexicalForm).getOrElse(""))
    val termFuture = App.executeSPARQLQuery(buildTermQuery(iri), termResult).map(_.headOption)
    val relsFuture = termRelationships(iri)
    for {
      termOpt <- termFuture
      relationships <- relsFuture
    } yield {
      termOpt.map {
        case (label, definition) => Term(iri, label, definition, relationships)
      }
    }
  }

  def orderBySearchedText[T <: LabeledTerm](terms: Seq[T], text: String): Seq[MatchedTerm[T]] = {
    val startsWithMatches = mutable.ListBuffer.empty[MatchedTerm[T]]
    val containingMatches = mutable.ListBuffer.empty[MatchedTerm[T]]
    val synonymMatches = mutable.ListBuffer.empty[MatchedTerm[T]]
    terms.foreach { term =>
      val lowerLabel = term.label.toLowerCase
      val lowerText = text.toLowerCase
      val location = lowerLabel.indexOf(lowerText)
      if (lowerLabel == lowerText) startsWithMatches += MatchedTerm(term, ExactMatch)
      else if (location == 0) startsWithMatches += MatchedTerm(term, PartialMatch)
      else if (location > 0) containingMatches += MatchedTerm(term, PartialMatch)
      else synonymMatches += MatchedTerm(term, BroadMatch)
    }
    (startsWithMatches.sortBy(_.term.label.toLowerCase) ++ containingMatches.sortBy(_.term.label.toLowerCase) ++ synonymMatches.sortBy(_.term.label.toLowerCase)).toList
  }

  def classification(iri: IRI, source: Option[IRI]): Future[Classification] = {
    def shouldHide(term: MinimalTerm) = {
      val termID = term.iri.toString
      termID.startsWith("http://example.org") ||
        termID == "http://www.w3.org/2002/07/owl#Nothing" ||
        termID == "http://www.w3.org/2002/07/owl#Thing"
    }
    val superclassesFuture = querySuperClasses(iri, source)
    val subclassesFuture = querySubClasses(iri, source)
    val equivalentsFuture = queryEquivalentClasses(iri, source)
    val termFuture = computedLabel(iri)
    for {
      term <- termFuture
      superclasses <- superclassesFuture
      subclasses <- subclassesFuture
      equivalents <- equivalentsFuture
    } yield Classification(term, superclasses.filterNot(shouldHide).toSet, subclasses.filterNot(shouldHide).toSet, equivalents.filterNot(shouldHide).toSet)
  }

  def querySuperClasses(iri: IRI, source: Option[IRI]): Future[Seq[MinimalTerm]] = {
    val definedByTriple = source.map(t('term, rdfsIsDefinedBy, _)).toList
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        (t(iri, rdfsSubClassOf, 'term) ::
          t('term, rdfsLabel, 'term_label) ::
          definedByTriple): _*))
    App.executeSPARQLQuery(query, fromMinimalQuerySolution)
  }

  def querySubClasses(iri: IRI, source: Option[IRI]): Future[Seq[MinimalTerm]] = {
    val definedByTriple = source.map(t('term, rdfsIsDefinedBy, _)).toList
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        (t('term, rdfsSubClassOf, iri) ::
          t('term, rdfsLabel, 'term_label) ::
          definedByTriple): _*))
    App.executeSPARQLQuery(query, fromMinimalQuerySolution)
  }

  def queryEquivalentClasses(iri: IRI, source: Option[IRI]): Future[Seq[MinimalTerm]] = {
    val definedByTriple = source.map(t('term, rdfsIsDefinedBy, _)).toList
    val union = new ElementUnion()
    union.addElement(bgp(t('term, owlEquivalentClass, iri)))
    union.addElement(bgp(t(iri, owlEquivalentClass, 'term)))
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        (t('term, rdfsLabel, 'term_label) ::
          definedByTriple): _*),
        union)
    App.executeSPARQLQuery(query, fromMinimalQuerySolution)
  }

  def allAncestors(iri: IRI): Future[Seq[MinimalTerm]] = {
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, new P_OneOrMore1(new P_Link(rdfsSubClassOf)), 'term),
        t('term, rdfsLabel, 'term_label)))
    App.executeSPARQLQuery(query, fromMinimalQuerySolution)
  }

  def allDescendants(iri: IRI): Future[Seq[MinimalTerm]] = {
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('term, new P_OneOrMore1(new P_Link(rdfsSubClassOf)), iri),
        t('term, rdfsLabel, 'term_label)))
    App.executeSPARQLQuery(query, fromMinimalQuerySolution)
  }

  def leastCommonSubsumers(iris: Iterable[IRI], source: Option[IRI]): Future[Seq[IRI]] = {
    def superClassTriple(iri: IRI) = t(iri, rdfsSubClassOf*, 'super)
    val definedByTriple = source.map(t('super, rdfsIsDefinedBy, _)).toList
    val superClassesQuery = select_distinct('super) from "http://kb.phenoscape.org/" where (
      bgp(
        (t('super, rdfType, owlClass) ::
          definedByTriple ++
          iris.map(superClassTriple).toList): _*))
    val superSuperClassesQuery = select_distinct('supersuper) from "http://kb.phenoscape.org/" where (
      bgp(
        (t('super, rdfType, owlClass) ::
          t('supersuper, rdfType, owlClass) ::
          t('super, new P_OneOrMore1(new P_Link(rdfsSubClassOf)), 'supersuper) ::
          definedByTriple ++
          iris.map(superClassTriple).toList): _*))
    val superClassesFuture = App.executeSPARQLQuery(superClassesQuery, _.getResource("super").getURI)
    val superSuperClassesFuture = App.executeSPARQLQuery(superSuperClassesQuery, _.getResource("supersuper").getURI)
    for {
      superClassesResult <- superClassesFuture
      superSuperClassesResult <- superSuperClassesFuture
    } yield {
      (superClassesResult.toSet -- superSuperClassesResult).toSeq.sorted.map(IRI.create)
    }
  }

  def resolveLabelExpression(expression: String): Validation[String, OWLClassExpression] = {
    val parser = new ManchesterOWLSyntaxClassExpressionParser(factory, SPARQLEntityChecker)
    try {
      Success(parser.parse(expression))
    } catch {
      case e: ParserException => Failure(e.getMessage)
    }
  }

  def buildTermQuery(iri: IRI): Query =
    select_distinct('label, 'definition) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'label)),
        optional(bgp(
          t(iri, definition, 'definition))))

  def termRelationships(iri: IRI): Future[Seq[TermRelationship]] =
    App.executeSPARQLQuery(buildRelationsQuery(iri), (result) => TermRelationship(
      MinimalTerm(
        IRI.create(result.getResource("relation").getURI),
        result.getLiteral("relation_name").getString),
      MinimalTerm(
        IRI.create(result.getResource("filler").getURI),
        result.getLiteral("filler_name").getString)))

  def buildRelationsQuery(iri: IRI): Query = {
    val query = select('relation, 'filler) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsSubClassOf, 'restriction),
        t('restriction, owlOnProperty, 'relation),
        t('relation, rdfsLabel, 'relation_label),
        t('restriction, owlSomeValuesFrom, 'filler),
        t('filler, rdfsLabel, 'filler_label)),
        new ElementFilter(new E_IsIRI(new ExprVar('relation))),
        new ElementFilter(new E_IsIRI(new ExprVar('filler))))
    // We need to handle multiple labels in the DB for properties (and possibly classes)
    query.getProject.add(Var.alloc("filler_name"), query.allocAggregate(new AggMin(new ExprVar('filler_label))))
    query.getProject.add(Var.alloc("relation_name"), query.allocAggregate(new AggMin(new ExprVar('relation_label))))
    query.addGroupBy("relation")
    query.addGroupBy("filler")
    query
  }

  def buildSearchQuery(text: String, termType: IRI, property: IRI): Query = {
    val searchText = if (text.endsWith("*")) text else s"$text*"
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('term_label, BDSearch, NodeFactory.createLiteral(searchText)),
        t('term_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('term_label, BDRank, 'rank),
        t('term, rdfsLabel, 'term_label),
        t('term, rdfType, termType)),
        new ElementFilter((new E_IsIRI(new ExprVar('term)))))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    query.setLimit(100)
    query
  }

  private def triplesBlock(elements: Element*): ElementGroup = {
    val block = new ElementGroup()
    elements.foreach(block.addElement)
    block
  }

  def buildOntologyTermQuery(text: String, definedBy: IRI, limit: Int): Query = {
    val searchText = if (text.endsWith("*")) text else s"$text*"
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('matched_label, BDSearch, NodeFactory.createLiteral(searchText)),
        t('matched_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
        t('matched_label, BDRank, 'rank),
        t('term, rdfsLabel | (hasExactSynonym | hasRelatedSynonym), 'matched_label),
        t('term, rdfsLabel, 'term_label),
        t('term, rdfsIsDefinedBy, definedBy),
        t('term, rdfType, owlClass)),
        new ElementFilter(new E_IsIRI(new ExprVar('term))),
        new ElementFilter(new E_NotExists(triplesBlock(bgp(t('term, owlDeprecated, "true" ^^ XSDDatatype.XSDboolean))))))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    if (limit > 0) query.setLimit(limit)
    query
  }

  def buildLabelQuery(iri: IRI): Query = {
    val query = select('term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'term_label)))
    query.setLimit(1)
    query
  }

  def buildLabelsQuery(iris: IRI*): Query = {
    val nodes = iris.map(iri => new NodeValueNode(NodeFactory.createURI(iri.toString)))
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('term, rdfsLabel, 'term_label)),
        new ElementFilter(new E_OneOf(new ExprVar('term), new ExprList(nodes))))
    query
  }

  implicit val JSONResultItemsMarshaller: ToEntityMarshaller[Seq[JSONResultItem]] = Marshaller.combined(results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson)))

  implicit val JSONResultItemMarshaller: ToEntityMarshaller[JSONResultItem] = Marshaller.combined(result =>
    result.toJSON)

  implicit val IRIMarshaller: ToEntityMarshaller[IRI] = Marshaller.combined(iri =>
    new JsObject(Map("@id" -> iri.toString.toJson)))

  implicit val IRIsMarshaller: ToEntityMarshaller[Seq[IRI]] = Marshaller.combined(results =>
    new JsObject(Map("results" -> results.map(iri => Map("@id" -> iri.toString.toJson)).toJson)))

  def fromMinimalQuerySolution(result: QuerySolution): MinimalTerm = MinimalTerm(
    IRI.create(result.getResource("term").getURI),
    result.getLiteral("term_label").getLexicalForm)

}

case class Term(iri: IRI, label: String, definition: String, relationships: Seq[TermRelationship]) extends LabeledTerm with JSONResultItem {

  def toJSON: JsObject = Map(
    "@id" -> iri.toString.toJson,
    "label" -> label.toJson,
    "definition" -> definition.toJson,
    "relationships" -> relationships.map(_.toJSON).toJson).toJson.asJsObject

}

case class MinimalTerm(iri: IRI, label: String) extends LabeledTerm with JSONResultItem {

  def toJSON: JsObject = Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject

}

case class SourcedMinimalTerm(term: MinimalTerm, sources: Set[IRI]) extends JSONResultItem {

  def toJSON: JsObject = (term.toJSON.fields +
    ("sources" -> sources.map(iri => Map("@id" -> iri.toString)).toJson)).toJson.asJsObject

}

case class MatchedTerm[T <: LabeledTerm](term: T, matchType: MatchType) extends JSONResultItem {

  def toJSON: JsObject = (term.toJSON.fields +
    ("matchType" -> matchType.toString.toJson)).toJson.asJsObject

}

sealed trait MatchType
case object ExactMatch extends MatchType {

  override val toString = "exact"

}
case object PartialMatch extends MatchType {

  override val toString = "partial"

}
case object BroadMatch extends MatchType {

  override val toString = "broad"

}

class LabelMapProvider(labels: Map[IRI, String]) extends ShortFormProvider {

  def getShortForm(entity: OWLEntity): String = labels.getOrElse(entity.getIRI, entity.getIRI.toString)

  def dispose(): Unit = Unit

}

case class Classification(term: MinimalTerm, superclasses: Set[MinimalTerm], subclasses: Set[MinimalTerm], equivalents: Set[MinimalTerm]) extends JSONResultItem {

  def toJSON: JsObject = JsObject(
    term.toJSON.fields ++
      Map("subClassOf" -> superclasses.toSeq.sortBy(_.label).map(_.toJSON).toJson,
        "superClassOf" -> subclasses.toSeq.sortBy(_.label).map(_.toJSON).toJson,
        "equivalentTo" -> equivalents.toSeq.sortBy(_.label).map(_.toJSON).toJson))

}

case class TermRelationship(property: MinimalTerm, value: MinimalTerm) extends JSONResultItem {

  def toJSON: JsObject = JsObject(
    "property" -> property.toJSON,
    "value" -> value.toJSON)

}

trait LabeledTerm extends JSONResultItem {

  def iri: IRI

  def label: String

}
