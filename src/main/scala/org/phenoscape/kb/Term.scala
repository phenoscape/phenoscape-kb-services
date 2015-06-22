package org.phenoscape.kb

import scala.concurrent.Future
import Main.system
import system.dispatcher
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
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.sparql.expr.E_NotExists
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.sparql.syntax.Element
import org.phenoscape.owl.util.ExpressionUtil
import java.util.regex.Pattern
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLEntity
import org.semanticweb.owlapi.util.ShortFormProvider
import uk.ac.manchester.cs.owl.owlapi.mansyntaxrenderer.ManchesterOWLSyntaxOWLObjectRendererImpl
import org.semanticweb.owlapi.model.OWLObject
import spray.client.pipelining._
import akka.util.Timeout
import scala.concurrent.duration._

object Term {

  private val factory = OWLManager.getOWLDataFactory

  def search(text: String, termType: IRI, property: IRI): Future[Seq[MinimalTerm]] = {
    App.executeSPARQLQuery(buildSearchQuery(text, termType, property), Term.fromMinimalQuerySolution)
  }

  def searchOntologyTerms(text: String, definedBy: IRI, limit: Int): Future[Seq[MinimalTerm]] = {
    App.executeSPARQLQuery(buildOntologyTermQuery(text, definedBy, limit), Term.fromMinimalQuerySolution)
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
    ExpressionUtil.expressionForName(Class(iri)).map { expression =>
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
    App.executeSPARQLQuery(buildTermQuery(iri), Term.fromQuerySolution(iri)).map(_.headOption)
  }

  def classification(iri: IRI): Future[Classification] = {
    implicit val timeout = Timeout(10 minutes)
    def shouldHide(term: MinimalTerm) = {
      val termID = term.iri.toString
      termID.startsWith("http://example.org") || termID == "http://www.w3.org/2002/07/owl#Nothing" || termID == "http://www.w3.org/2002/07/owl#Thing"
    }
    val pipeline = sendReceive ~> unmarshal[JsObject]
    val query = Uri.Query("object" -> s"<$iri>", "direct" -> "true")
    def toTerm(list: JsValue): Future[Set[MinimalTerm]] =
      Future.sequence(list.convertTo[List[String]].toSet[String].map(IRI.create).map(computedLabel))
    val superclassesFuture = pipeline(Get(App.Owlery.copy(path = App.Owlery.path / "superclasses", query = query)))
      .map(_.fields("subClassOf")).flatMap(toTerm)
    val subclassesFuture = pipeline(Get(App.Owlery.copy(path = App.Owlery.path / "subclasses", query = query)))
      .map(_.fields("superClassOf")).flatMap(toTerm)
    val equivalentsFuture = pipeline(Get(App.Owlery.copy(path = App.Owlery.path / "equivalent", query = query)))
      .map(_.fields("equivalentClass")).flatMap(toTerm)
    val termFuture = computedLabel(iri)
    for {
      term <- termFuture
      superclasses <- superclassesFuture
      subclasses <- subclassesFuture
      equivalents <- equivalentsFuture
    } yield Classification(term, superclasses.filterNot(shouldHide), subclasses.filterNot(shouldHide), equivalents.filterNot(shouldHide))
  }

  def buildTermQuery(iri: IRI): Query = {
    select('label, 'definition) from "http://kb.phenoscape.org/" where (
      bgp(
        t(iri, rdfsLabel, 'label)),
        optional(
          bgp(
            t(iri, definition, 'definition))))
  }

  def buildSearchQuery(text: String, termType: IRI, property: IRI): Query = {
    val query = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('term_label, BDSearch, NodeFactory.createLiteral(text)),
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
        new ElementFilter((new E_IsIRI(new ExprVar('term)))),
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

  implicit val JSONResultItemsMarshaller = Marshaller.delegate[Seq[JSONResultItem], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(_.toJSON).toJson))
  }

  implicit val JSONResultItemMarshaller = Marshaller.delegate[JSONResultItem, JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { result =>
    result.toJSON
  }

  implicit val IRIMarshaller = Marshaller.delegate[IRI, JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { iri =>
    new JsObject(Map("@id" -> iri.toString.toJson))
  }

  implicit val IRIsMarshaller = Marshaller.delegate[Seq[IRI], JsObject](App.`application/ld+json`, MediaTypes.`application/json`) { results =>
    new JsObject(Map("results" -> results.map(iri => Map("@id" -> iri.toString.toJson)).toJson))
  }

  def fromQuerySolution(iri: IRI)(result: QuerySolution): Term = Term(iri,
    result.getLiteral("label").getLexicalForm,
    Option(result.getLiteral("definition")).map(_.getLexicalForm).getOrElse(""))

  def fromMinimalQuerySolution(result: QuerySolution): MinimalTerm = MinimalTerm(
    IRI.create(result.getResource("term").getURI),
    result.getLiteral("term_label").getLexicalForm)

}

case class Term(iri: IRI, label: String, definition: String) extends JSONResultItem {

  def toJSON: JsObject = Map("@id" -> iri.toString, "label" -> label, "definition" -> definition).toJson.asJsObject

}

case class MinimalTerm(iri: IRI, label: String) extends JSONResultItem {

  def toJSON: JsObject = Map("@id" -> iri.toString, "label" -> label).toJson.asJsObject

}

case class SourcedMinimalTerm(term: MinimalTerm, sources: Set[IRI]) extends JSONResultItem {

  def toJSON: JsObject = (term.toJSON.fields +
    ("sources" -> sources.map(iri => Map("@id" -> iri.toString)).toJson)).toJson.asJsObject

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
