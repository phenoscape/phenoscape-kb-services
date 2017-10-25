package org.phenoscape.kb

import scala.collection.JavaConverters._
import scala.concurrent.Future

import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.Query
import org.apache.jena.query.QuerySolution
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.BindingFactory
import org.apache.jena.sparql.expr.E_Equals
import org.apache.jena.sparql.expr.E_IsIRI
import org.apache.jena.sparql.expr.E_LogicalOr
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.syntax.ElementFilter
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI

import KBVocab._
import KBVocab.rdfsLabel
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._

object AnatomicalEntity {

  private val dcSource = ObjectProperty(IRI.create("http://purl.org/dc/elements/1.1/source"))
  private val ECO = IRI.create("http://purl.obolibrary.org/obo/eco.owl")

  def homologyAnnotations(term: IRI, includeSubClasses: Boolean): Future[Seq[HomologyAnnotation]] = App.executeSPARQLQueryString(homologyAnnotationQuery(term, includeSubClasses), HomologyAnnotation(_, term))

  private def homologyAnnotationQuery(term: IRI, includeSubClasses: Boolean): String = {
    val termSpec = if (includeSubClasses) sparql"GRAPH $KBClosureGraph { ?term $rdfsSubClassOf $term . } "
    else sparql"VALUES ?term { $term }"
    val query = sparql"""
      SELECT DISTINCT ?subject ?object ?subjectTaxon ?subjectVTO ?objectTaxon ?objectVTO ?negated ?source ?evidenceType ?relation
      FROM $KBMainGraph
      WHERE {
        VALUES ?relation { $homologous_to $serially_homologous_to }
        $termSpec
        ?annotation ?associationHasPredicate ?relation .
        ?annotation $associationHasSubject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?subject .
        ?annotation $associationHasSubject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?subjectTaxonRestriction .
        ?subjectTaxonRestriction $owlOnProperty $in_taxon .
        ?subjectTaxonRestriction $owlSomeValuesFrom ?subjectTaxon .
        ?annotation $associationHasObject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?object .
        ?annotation $associationHasObject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?objectTaxonRestriction .
        ?objectTaxonRestriction $owlOnProperty $in_taxon .
        ?objectTaxonRestriction $owlSomeValuesFrom ?objectTaxon .
        ?annotation $associationIsNegated ?negated .
        ?annotation $has_evidence ?evidence .
        ?evidence $dcSource ?source .
        ?evidence $rdfType ?evidenceType .
        ?evidenceType $rdfsIsDefinedBy $ECO .
        FILTER(isIRI(?subject))
        FILTER(isIRI(?object))
        FILTER((?subject = ?term) || (?object = ?term))
        OPTIONAL {
          ?subjectTaxon $owlEquivalentClass ?subjectVTO .
          ?subjectVTO $rdfsIsDefinedBy $VTO .
          FILTER(isIRI(?subjectVTO))
        }
        OPTIONAL {
          ?objectTaxon $owlEquivalentClass ?objectVTO .
          ?objectVTO $rdfsIsDefinedBy $VTO .
          FILTER(isIRI(?objectVTO))
        }
      }
      """
    query.text
  }

}

final case class HomologyAnnotation(subject: IRI, subjectTaxon: IRI, `object`: IRI, objectTaxon: IRI, source: String, evidence: IRI, negated: Boolean, relation: IRI) extends JSONResultItem {

  def toJSON: JsObject = Map(
    "subject" -> subject.toString.toJson,
    "subjectTaxon" -> subjectTaxon.toString.toJson,
    "object" -> `object`.toString.toJson,
    "objectTaxon" -> objectTaxon.toString.toJson,
    "source" -> source.toJson,
    "negated" -> negated.toJson,
    "evidence" -> evidence.toString.toJson,
    "relation" -> relation.toString.toJson)
    .toJson.asJsObject

}

object HomologyAnnotation {

  def apply(querySolution: QuerySolution, queriedTerm: IRI): HomologyAnnotation = {
    val querySubject = IRI.create(querySolution.getResource("subject").getURI)
    val queryObject = IRI.create(querySolution.getResource("object").getURI)
    val querySubjectTaxon = {
      val st = querySolution.getResource("subjectTaxon").getURI
      if ((!st.startsWith("http://purl.obolibrary.org/obo/VTO_")) && querySolution.contains("subjectVTO"))
        IRI.create(querySolution.getResource("subjectVTO").getURI)
      else IRI.create(st)
    }
    val queryObjectTaxon = {
      val st = querySolution.getResource("objectTaxon").getURI
      if ((!st.startsWith("http://purl.obolibrary.org/obo/VTO_")) && querySolution.contains("objectVTO"))
        IRI.create(querySolution.getResource("objectVTO").getURI)
      else IRI.create(st)
    }
    val (annotationSubject, annotationSubjectTaxon, annotationObject, annotationObjectTaxon) = if (querySubject == queriedTerm) (querySubject, querySubjectTaxon, queryObject, queryObjectTaxon)
    else (queryObject, queryObjectTaxon, querySubject, querySubjectTaxon)
    HomologyAnnotation(
      annotationSubject,
      annotationSubjectTaxon,
      annotationObject,
      annotationObjectTaxon,
      querySolution.getLiteral("source").getLexicalForm,
      IRI.create(querySolution.getResource("evidenceType").getURI),
      querySolution.getLiteral("negated").getBoolean,
      IRI.create(querySolution.getResource("relation").getURI))
  }

}