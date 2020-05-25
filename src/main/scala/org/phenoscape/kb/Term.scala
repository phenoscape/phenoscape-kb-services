package org.phenoscape.kb

import java.util.regex.Pattern

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.{Query, QueryFactory, QuerySolution}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr._
import org.apache.jena.sparql.expr.aggregate.AggMin
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.path.{P_Link, P_OneOrMore1}
import org.apache.jena.sparql.syntax.{Element, ElementFilter, ElementGroup, ElementUnion}
import org.phenoscape.kb.KBVocab.{rdfsLabel, rdfsSubClassOf, _}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.kb.ingest.util.ExpressionUtil
import org.phenoscape.kb.util.SPARQLEntityChecker
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab._
import org.phenoscape.owl.util.ExpressionsUtil
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.manchestersyntax.parser.ManchesterOWLSyntaxClassExpressionParser
import org.semanticweb.owlapi.manchestersyntax.renderer.{ManchesterOWLSyntaxOWLObjectRendererImpl, ParserException}
import org.semanticweb.owlapi.model.{IRI, OWLClassExpression, OWLEntity, OWLObject}
import org.semanticweb.owlapi.util.ShortFormProvider
import scalaz._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.postfixOps

object Term {

  private val factory = OWLManager.getOWLDataFactory

  private val PartOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)

  def search(text: String,
             termType: IRI,
             properties: Seq[IRI],
             definedBys: Seq[IRI],
             includeDeprecated: Boolean = false,
             limit: Int = 100): Future[Seq[MatchedTerm[DefinedMinimalTerm]]] = {
    def resultFromQuerySolution(qs: QuerySolution): DefinedMinimalTerm =
      DefinedMinimalTerm(MinimalTerm.fromQuerySolution(qs),
                         Option(qs.getResource("ont")).map(o => IRI.create(o.getURI)))

    App
      .executeSPARQLQueryString(
        buildTermSearchQuery(text, termType, properties.toList, definedBys, includeDeprecated, limit).text,
        resultFromQuerySolution)
      .map(categorizeSearchedText(_, text).distinct)
  }

  def searchOntologyTerms(text: String, definedBy: IRI, limit: Int): Future[Seq[MatchedTerm[MinimalTerm]]] =
    App
      .executeSPARQLQuery(buildOntologyTermQuery(text, definedBy, limit), MinimalTerm.fromQuerySolution)
      .map(orderBySearchedText(_, text))

  def label(iri: IRI): Future[Option[MinimalTerm]] =
    App.executeSPARQLQuery(buildLabelQuery(iri), MinimalTerm.fromQuerySolution).map(_.headOption)

  def labels(iris: IRI*): Future[Seq[MinimalTerm]] =
    App.executeSPARQLQuery(buildLabelsQuery(iris: _*), MinimalTerm.fromQuerySolution)

  def computedLabel(iri: IRI): Future[MinimalTerm] =
    (for {
      labelOpt <- label(iri)
    } yield labelOpt.map(Future.successful).getOrElse {
      computeLabelForAnonymousTerm(iri)
    }).flatMap(identity)

  def stateLabelForPhenotype(phenotype: IRI): Future[Option[MinimalTerm]] = {
    val query = select_distinct('state, 'state_desc) from "http://kb.phenoscape.org/" where (bgp(
      t('state, dcDescription, 'state_desc),
      t('state, describes_phenotype, phenotype)))
    for {
      res <- App.executeSPARQLQuery(
               query,
               result => MinimalTerm(phenotype, Some(result.getLiteral("state_desc").getLexicalForm)))
    } yield res.headOption
  }

  def computeLabelForAnonymousTerm(iri: IRI): Future[MinimalTerm] =
    iri.toString match {
      case expression
          if expression.startsWith(ExpressionUtil.namedExpressionPrefix) || expression.startsWith(
            ExpressionUtil.namedSubClassPrefix)                                 =>
        labelForNamedExpression(iri)
      case negation if negation.startsWith("http://phenoscape.org/not/")        =>
        computedLabel(IRI.create(negation.replaceFirst(Pattern.quote("http://phenoscape.org/not/"), ""))).map { term =>
          MinimalTerm(iri, term.label.map(l => s"not $l"))
        }
      case absence if absence.startsWith("http://phenoscape.org/not_has_part/") =>
        computedLabel(IRI.create(absence.replaceFirst(Pattern.quote("http://phenoscape.org/not_has_part/"), ""))).map {
          term =>
            MinimalTerm(iri, term.label.map(l => s"absence of $l"))
        }
      case _                                                                    => Future.successful(MinimalTerm(iri, Some(iri.toString)))
    }

  def labelForNamedExpression(iri: IRI): Future[MinimalTerm] =
    ExpressionsUtil
      .expressionForName(Class(iri))
      .map { expression =>
        for {
          terms   <- Future.sequence(expression.getSignature.asScala.map(term => computedLabel(term.getIRI)))
          labelMap = terms.map(term => term.iri -> term.label.getOrElse(term.iri.toString)).toMap
        } yield {
          val renderer = createEntityRenderer(new LabelMapProvider(labelMap))
          MinimalTerm(iri, Some(renderer(expression)))
        }
      }
      .getOrElse(Future.successful(MinimalTerm(iri, Some(iri.toString))))

  private def createEntityRenderer(shortFormProvider: ShortFormProvider): OWLObject => String = {
    val renderer = new ManchesterOWLSyntaxOWLObjectRendererImpl()
    renderer.setShortFormProvider(shortFormProvider)
    entity => renderer.render(entity).replaceAll("\n", " ").replaceAll("\\s+", " ")
  }

  def withIRI(iri: IRI): Future[Option[Term]] = {
    def termResult(result: QuerySolution) =
      (Option(result.getLiteral("label")).map(_.getLexicalForm),
       Option(result.getLiteral("definition")).map(_.getLexicalForm).getOrElse(""),
       Option(result.getResource("ontology")).map(_.getURI))

    val termFuture     = App.executeSPARQLQuery(buildTermQuery(iri), termResult).map(_.headOption)
    val synonymsFuture = termSynonyms(iri)
    val relsFuture     = termRelationships(iri)
    for {
      termOpt       <- termFuture
      synonyms      <- synonymsFuture
      relationships <- relsFuture
    } yield termOpt.map {
      case (label, definition, ontology) => Term(iri, label, definition, ontology, synonyms, relationships)
    }
  }

  private[this] def categorizeSearchedText[T <: LabeledTerm](terms: Seq[T], text: String): Seq[MatchedTerm[T]] =
    terms.map { term =>
      term.label match {
        case Some(label) =>
          val lowerLabel = label.toLowerCase
          val lowerText  = text.toLowerCase
          val location   = lowerLabel.indexOf(lowerText)
          if (lowerLabel == lowerText) MatchedTerm(term, ExactMatch)
          else if (location == 0) MatchedTerm(term, PartialMatch)
          else if (location > 0) MatchedTerm(term, PartialMatch)
          else MatchedTerm(term, BroadMatch)
        case None        => MatchedTerm(term, BroadMatch)
      }
    }

  def orderBySearchedText[T <: LabeledTerm](terms: Seq[T], text: String): Seq[MatchedTerm[T]] = {
    val startsWithMatches = mutable.ListBuffer.empty[MatchedTerm[T]]
    val containingMatches = mutable.ListBuffer.empty[MatchedTerm[T]]
    val synonymMatches    = mutable.ListBuffer.empty[MatchedTerm[T]]
    terms.foreach { term =>
      term.label match {
        case Some(label) =>
          val lowerLabel = label.toLowerCase
          val lowerText  = text.toLowerCase
          val location   = lowerLabel.indexOf(lowerText)
          if (lowerLabel == lowerText) startsWithMatches += MatchedTerm(term, ExactMatch)
          else if (location == 0) startsWithMatches += MatchedTerm(term, PartialMatch)
          else if (location > 0) containingMatches += MatchedTerm(term, PartialMatch)
          else synonymMatches += MatchedTerm(term, BroadMatch)
        case None        => synonymMatches += MatchedTerm(term, BroadMatch)
      }
    }
    (startsWithMatches.sortBy(_.term.label.map(_.toLowerCase)) ++ containingMatches.sortBy(
      _.term.label.map(_.toLowerCase)) ++ synonymMatches.sortBy(_.term.label.map(_.toLowerCase))).toList
  }

  def classification(iri: IRI, source: Option[IRI]): Future[Classification] = {
    def shouldHide(term: MinimalTerm) = {
      val termID = term.iri.toString
      termID.startsWith("http://example.org") ||
      termID == "http://www.w3.org/2002/07/owl#Nothing" ||
      termID == "http://www.w3.org/2002/07/owl#Thing"
    }

    val superclassesFuture = querySuperClasses(iri, source)
    val subclassesFuture   = querySubClasses(iri, source)
    val equivalentsFuture  = queryEquivalentClasses(iri, source)
    val termFuture         = computedLabel(iri)
    for {
      term         <- termFuture
      superclasses <- superclassesFuture
      subclasses   <- subclassesFuture
      equivalents  <- equivalentsFuture
    } yield Classification(term,
                           superclasses.filterNot(shouldHide).toSet,
                           subclasses.filterNot(shouldHide).toSet,
                           equivalents.filterNot(shouldHide).toSet)
  }

  def querySuperClasses(iri: IRI, source: Option[IRI]): Future[Seq[MinimalTerm]] = {
    val definedByTriple = source.map(t('term, rdfsIsDefinedBy, _)).toList
    val query           = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (bgp(
      (t(iri, rdfsSubClassOf, 'term) ::
        t('term, rdfsLabel, 'term_label) ::
        definedByTriple): _*))
    App.executeSPARQLQuery(query, MinimalTerm.fromQuerySolution)
  }

  def querySubClasses(iri: IRI, source: Option[IRI]): Future[Seq[MinimalTerm]] = {
    val definedByTriple = source.map(t('term, rdfsIsDefinedBy, _)).toList
    val query           = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (bgp(
      (t('term, rdfsSubClassOf, iri) ::
        t('term, rdfsLabel, 'term_label) ::
        definedByTriple): _*))
    App.executeSPARQLQuery(query, MinimalTerm.fromQuerySolution)
  }

  def queryAnatomySubClasses(iri: IRI,
                             source: IRI,
                             byPartOf: Boolean,
                             byHistoricalHomolog: Boolean,
                             bySerialHomolog: Boolean): Future[Seq[MinimalTerm]] = {
    val partOfSome = NamedRestrictionGenerator.getClassRelationIRI(part_of.getIRI)
    val direct     =
      sparql"""
      {
        ?term $rdfsSubClassOf $iri .
      }
      """.text
    val partOf     =
      if (byPartOf) List(sparql"""
      {
        ?partOfClass $partOfSome $iri .
        ?term $rdfsSubClassOf ?partOfClass .
      }
     """.text)
      else Nil
    val pieces     = QueryText((direct :: partOf).mkString(" UNION "))
    val query      =
      sparql"""
      SELECT DISTINCT ?term ?term_label
      FROM $KBMainGraph
      WHERE {
        $pieces
        ?term $rdfsIsDefinedBy $source .
        ?term $RDFSLabel ?term_label .
        FILTER(isIRI(?term))
      }
      """
    App.executeSPARQLQueryString(query.text, MinimalTerm.fromQuerySolution)
  }

  def queryEquivalentClasses(iri: IRI, source: Option[IRI]): Future[Seq[MinimalTerm]] = {
    val definedByTriple = source.map(t('term, rdfsIsDefinedBy, _)).toList
    val union           = new ElementUnion()
    union.addElement(bgp(t('term, owlEquivalentClass, iri)))
    union.addElement(bgp(t(iri, owlEquivalentClass, 'term)))
    val query           = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (bgp(
      (t('term, rdfsLabel, 'term_label) ::
        definedByTriple): _*),
    union)
    App.executeSPARQLQuery(query, MinimalTerm.fromQuerySolution)
  }

  def allAncestors(iri: IRI, includePartOf: Boolean): Future[Seq[MinimalTerm]] = {
    val superclasses =
      sparql"""
            GRAPH $KBClosureGraph {
              $iri $rdfsSubClassOf ?term .
              FILTER($iri != ?term)
            }
            """
    val partOfs      =
      sparql"""
            ?container $PartOfSome ?term .
            GRAPH $KBClosureGraph {
              $iri $rdfsSubClassOf ?container .
              FILTER($iri != ?container)
            }
            """
    val all          = if (includePartOf) sparql" { $superclasses } UNION { $partOfs } " else superclasses
    val query        =
      sparql"""
            SELECT ?term (MIN(?term_label_n) AS ?term_label)
            FROM $KBMainGraph
            WHERE {
              $all
              OPTIONAL { ?term $rdfsLabel ?term_label_n . }
            }
            GROUP BY ?term
            """
    App.executeSPARQLQueryString(query.text, MinimalTerm.fromQuerySolution)
  }

  def allDescendants(iri: IRI, includeParts: Boolean): Future[Seq[MinimalTerm]] = {
    val subclasses =
      sparql"""
            GRAPH $KBClosureGraph {
              ?term $rdfsSubClassOf $iri .
              FILTER($iri != ?term)
            }
            """
    val parts      =
      sparql"""
            ?query $PartOfSome $iri .
            GRAPH $KBClosureGraph {
              ?term $rdfsSubClassOf ?query .
              FILTER(?query != ?term)
            }
            """
    val all        = if (includeParts) sparql" { $subclasses } UNION { $parts } " else subclasses
    val query      =
      sparql"""
            SELECT ?term (MIN(?term_label_n) AS ?term_label)
            FROM $KBMainGraph
            WHERE {
              $all
              OPTIONAL { ?term $rdfsLabel ?term_label_n . }
            }
            GROUP BY ?term
            """
    App.executeSPARQLQueryString(query.text, MinimalTerm.fromQuerySolution)
  }

  def leastCommonSubsumers(iris: Iterable[IRI], source: Option[IRI]): Future[Seq[IRI]] = {
    def superClassTriple(iri: IRI) = t(iri, rdfsSubClassOf *, 'super)

    val definedByTriple         = source.map(t('super, rdfsIsDefinedBy, _)).toList
    val superClassesQuery       = select_distinct('super) from "http://kb.phenoscape.org/" where (bgp(
      (t('super, rdfType, owlClass) ::
        definedByTriple ++
        iris.map(superClassTriple).toList): _*))
    val superSuperClassesQuery  = select_distinct('supersuper) from "http://kb.phenoscape.org/" where (
      bgp(
        (t('super, rdfType, owlClass) ::
          t('supersuper, rdfType, owlClass) ::
          t('super, new P_OneOrMore1(new P_Link(rdfsSubClassOf)), 'supersuper) ::
          definedByTriple ++
          iris.map(superClassTriple).toList): _*
      )
    )
    val superClassesFuture      = App.executeSPARQLQuery(superClassesQuery, _.getResource("super").getURI)
    val superSuperClassesFuture = App.executeSPARQLQuery(superSuperClassesQuery, _.getResource("supersuper").getURI)
    for {
      superClassesResult      <- superClassesFuture
      superSuperClassesResult <- superSuperClassesFuture
    } yield (superClassesResult.toSet -- superSuperClassesResult).toSeq.sorted.map(IRI.create)
  }

  def resolveLabelExpression(expression: String): Validation[String, OWLClassExpression] = {
    val parser = new ManchesterOWLSyntaxClassExpressionParser(factory, SPARQLEntityChecker)
    try Success(parser.parse(expression))
    catch {
      case e: ParserException => Failure(e.getMessage)
    }
  }

  def buildTermQuery(iri: IRI): Query =
    select_distinct('label, 'definition, 'ontology) from "http://kb.phenoscape.org/" where (optional(
      bgp(t(iri, rdfsLabel, 'label))),
    optional(bgp(t(iri, definition, 'definition))),
    optional(bgp(t(iri, rdfsIsDefinedBy, 'ontology))))

  def termRelationships(iri: IRI): Future[Seq[TermRelationship]] =
    App.executeSPARQLQuery(
      buildRelationsQuery(iri),
      (result) =>
        TermRelationship(
          MinimalTerm(IRI.create(result.getResource("relation").getURI),
                      Some(result.getLiteral("relation_name").getString)),
          MinimalTerm(IRI.create(result.getResource("filler").getURI), Some(result.getLiteral("filler_name").getString))
        )
    )

  def termSynonyms(iri: IRI): Future[Seq[(IRI, String)]] = {
    val query =
      sparql"""
      SELECT DISTINCT ?relation ?synonym
      FROM $KBMainGraph
      WHERE {
        VALUES ?relation {
      		  $hasExactSynonym
      		  $hasRelatedSynonym
      		  $hasNarrowSynonym
      		  $hasBroadSynonym
          }
    		  $iri ?relation ?synonym .
      }
      """
    App.executeSPARQLQueryString(
      query.text,
      result => IRI.create(result.getResource("relation").getURI) -> result.getLiteral("synonym").getLexicalForm)
  }

  def buildRelationsQuery(iri: IRI): Query = {
    val query = select('relation, 'filler) from "http://kb.phenoscape.org/" where (bgp(
      t(iri, rdfsSubClassOf, 'restriction),
      t('restriction, owlOnProperty, 'relation),
      t('relation, rdfsLabel, 'relation_label),
      t('restriction, owlSomeValuesFrom, 'filler),
      t('filler, rdfsLabel, 'filler_label)
    ),
    new ElementFilter(new E_IsIRI(new ExprVar('relation))),
    new ElementFilter(new E_IsIRI(new ExprVar('filler))))
    // We need to handle multiple labels in the DB for properties (and possibly classes)
    query.getProject.add(Var.alloc("filler_name"), query.allocAggregate(new AggMin(new ExprVar('filler_label))))
    query.getProject.add(Var.alloc("relation_name"), query.allocAggregate(new AggMin(new ExprVar('relation_label))))
    query.addGroupBy("relation")
    query.addGroupBy("filler")
    query
  }

  def buildTermSearchQuery(text: String,
                           termType: IRI,
                           properties: List[IRI],
                           definedBys: Seq[IRI],
                           includeDeprecated: Boolean = false,
                           limit: Int = 100): QueryText = {
    import scalaz.Scalaz._
    val searchText       = if (text.endsWith("*")) text else s"$text*"
    val deprecatedFilter =
      if (includeDeprecated) sparql"" else sparql" FILTER NOT EXISTS { ?term $owlDeprecated true . } "
    val definedByPattern = if (definedBys.nonEmpty) {
      val definedByValues = definedBys.map(ont => sparql" $ont ").reduce(_ + _)
      sparql"""
                VALUES ?definingOnt { $definedByValues }
                ?term $rdfsIsDefinedBy ?definingOnt .
            """
    } else sparql""
    val termToTextRel    = (if (properties.nonEmpty) properties else List(RDFSLabel.getIRI))
      .map(p => sparql"$p")
      .intersperse(sparql" | ")
      .reduce(_ + _)
    sparql"""
            SELECT DISTINCT ?term ?term_label ?ont
            FROM $KBMainGraph
            WHERE {
              ?matched_label $BDSearch $searchText .
              ?matched_label $BDMatchAllTerms true .
              ?matched_label $BDRank ?rank .
              BIND(IF(?matched_label = $text, 0, ?rank) AS ?boosted_rank)
              ?term $termToTextRel ?matched_label .
              ?term $RDFSLabel ?term_label .
              ?term $rdfType $termType .
              $definedByPattern
              OPTIONAL { ?term $rdfsIsDefinedBy ?ont . }
              FILTER(isIRI(?term))
              $deprecatedFilter
            }
            ORDER BY ASC(?boosted_rank)
            LIMIT $limit
          """
  }

  private def triplesBlock(elements: Element*): ElementGroup = {
    val block = new ElementGroup()
    elements.foreach(block.addElement)
    block
  }

  def buildOntologyTermQuery(text: String, definedBy: IRI, limit: Int): Query = {
    val searchText = if (text.endsWith("*")) text else s"$text*"
    val query      = select_distinct('term, 'term_label) from "http://kb.phenoscape.org/" where (bgp(
      t('matched_label, BDSearch, NodeFactory.createLiteral(searchText)),
      t('matched_label, BDMatchAllTerms, NodeFactory.createLiteral("true")),
      t('matched_label, BDRank, 'rank),
      t('term,
        rdfsLabel | (hasExactSynonym | (hasRelatedSynonym | (hasNarrowSynonym | hasBroadSynonym))),
        'matched_label),
      t('term, rdfsLabel, 'term_label),
      t('term, rdfsIsDefinedBy, definedBy),
      t('term, rdfType, owlClass)
    ),
    new ElementFilter(new E_IsIRI(new ExprVar('term))),
    new ElementFilter(new E_NotExists(triplesBlock(bgp(t('term, owlDeprecated, "true" ^^ XSDDatatype.XSDboolean))))))
    query.addOrderBy('rank, Query.ORDER_ASCENDING)
    if (limit > 0) query.setLimit(limit)
    query
  }

  def buildLabelQuery(iri: IRI): Query = {
    val query =
      sparql"""
        SELECT ?term ?term_label
        FROM $KBMainGraph
        WHERE {
          VALUES ?term { $iri }
          ?term $rdfsLabel ?term_label .
        }
        LIMIT 1
       """
    query.toQuery
  }

  def buildLabelsQuery(iris: IRI*): Query = {
    val terms                = iris.map(iri => sparql" $iri ").fold(sparql"")(_ + _)
    val queryText: QueryText =
      sparql"""
    SELECT ?term (MIN(?term_lbl) AS ?term_label)
    FROM $KBMainGraph
    WHERE {
      VALUES ?term { $terms }
      OPTIONAL {
        ?term $rdfsLabel ?term_lbl .
      }
    } GROUP BY ?term
    """
    QueryFactory.create(queryText.text)
  }

  implicit val IRIMarshaller: ToEntityMarshaller[IRI]       =
    Marshaller.combined(iri => new JsObject(Map("@id" -> iri.toString.toJson)))

  implicit val IRIsMarshaller: ToEntityMarshaller[Seq[IRI]] = Marshaller.combined(results =>
    new JsObject(Map("results" -> results.map(iri => Map("@id" -> iri.toString.toJson)).toJson)))

}

final case class Term(iri: IRI,
                      label: Option[String],
                      definition: String,
                      sourceOntology: Option[String],
                      synonyms: Seq[(IRI, String)],
                      relationships: Seq[TermRelationship])
    extends LabeledTerm
    with JSONResultItem {

  def toJSON: JsObject =
    Map(
      "@id"           -> iri.toString.toJson,
      "label"         -> label.map(_.toJson).getOrElse(JsNull),
      "definition"    -> definition.toJson,
      "isDefinedBy"   -> sourceOntology.map(_.toJson).getOrElse(JsNull),
      "synonyms"      -> synonyms.map {
        case (iri, value) => JsObject("property" -> iri.toString.toJson, "value" -> value.toJson).toJson
      }.toJson,
      "relationships" -> relationships.map(_.toJSON).toJson
    ).toJson.asJsObject

}

final case class MinimalTerm(iri: IRI, label: Option[String]) extends LabeledTerm with JSONResultItem {

  def toJSON: JsObject =
    Map("@id" -> iri.toString.toJson, "label" -> label.map(_.toJson).getOrElse(JsNull)).toJson.asJsObject

  def toTSV: String    = s"$iri\t${label.getOrElse("")}"

}

object MinimalTerm {

  def fromQuerySolution(result: QuerySolution): MinimalTerm =
    MinimalTerm(IRI.create(result.getResource("term").getURI),
                Option(result.getLiteral("term_label")).map(_.getLexicalForm))

  val tsvMarshaller: ToEntityMarshaller[MinimalTerm] =
    Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose(_.toTSV)

  val tsvSeqMarshaller: ToEntityMarshaller[Seq[MinimalTerm]] =
    Marshaller.stringMarshaller(MediaTypes.`text/tab-separated-values`).compose { terms =>
      val header = "IRI\tlabel"
      s"$header\n${terms.map(_.toTSV).mkString("\n")}"
    }

  implicit val comboSeqMarshaller: ToEntityMarshaller[Seq[MinimalTerm]] =
    Marshaller.oneOf(tsvSeqMarshaller, JSONResultItem.JSONResultItemsMarshaller)

}

final case class SourcedMinimalTerm(term: MinimalTerm, sources: Set[IRI]) extends JSONResultItem {

  def toJSON: JsObject =
    (term.toJSON.fields +
      ("sources" -> sources.map(iri => Map("@id" -> iri.toString)).toJson)).toJson.asJsObject

}

final case class DefinedMinimalTerm(term: MinimalTerm, definedBy: Option[IRI]) extends LabeledTerm with JSONResultItem {

  def iri: IRI = term.iri

  def label: Option[String] = term.label

  def toJSON: JsObject = {
    val extra = definedBy.map("isDefinedBy" -> _.toString.toJson).toList
    (term.toJSON.fields ++ extra).toJson.asJsObject
  }

}

final case class MatchedTerm[T <: LabeledTerm](term: T, matchType: MatchType) extends JSONResultItem {

  def toJSON: JsObject =
    (term.toJSON.fields +
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

final case class Classification(term: MinimalTerm,
                                superclasses: Set[MinimalTerm],
                                subclasses: Set[MinimalTerm],
                                equivalents: Set[MinimalTerm])
    extends JSONResultItem {

  def toJSON: JsObject =
    JsObject(
      term.toJSON.fields ++
        Map(
          "subClassOf"   -> superclasses.toSeq.sortBy(_.label.map(_.toLowerCase)).map(_.toJSON).toJson,
          "superClassOf" -> subclasses.toSeq.sortBy(_.label.map(_.toLowerCase)).map(_.toJSON).toJson,
          "equivalentTo" -> equivalents.toSeq.sortBy(_.label.map(_.toLowerCase)).map(_.toJSON).toJson
        )
    )

}

final case class TermRelationship(property: MinimalTerm, value: MinimalTerm) extends JSONResultItem {

  def toJSON: JsObject = JsObject("property" -> property.toJSON, "value" -> value.toJSON)

}

trait LabeledTerm extends JSONResultItem {

  def iri: IRI

  def label: Option[String]

}
