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
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI

import KBVocab._
import KBVocab.rdfsLabel
import spray.json._
import spray.json.DefaultJsonProtocol._

object AnatomicalEntity {

  private val dcSource = ObjectProperty(IRI.create("http://purl.org/dc/elements/1.1/source"))
  private val ECO = IRI.create("http://purl.obolibrary.org/obo/eco.owl")

  def homologyAnnotations(term: IRI): Future[Seq[HomologyAnnotation]] = App.executeSPARQLQuery(homologyAnnotationQuery(term), HomologyAnnotation(_, term))

  private def homologyAnnotationQuery(term: IRI): Query = {
    val query = select_distinct('subject, 'object, 'subjectTaxon, 'subjectVTO, 'objectTaxon, 'objectVTO, 'negated, 'source, 'evidenceType, 'relation) from "http://kb.phenoscape.org/" where (
      bgp(
        t('annotation, associationHasPredicate, 'relation),
        t('annotation, associationHasSubject / rdfType / owlIntersectionOf / rdfRest.* / rdfFirst, 'subject),
        t('annotation, associationHasSubject / rdfType / owlIntersectionOf / rdfRest.* / rdfFirst, 'subjectTaxonRestriction),
        t('subjectTaxonRestriction, owlOnProperty, in_taxon),
        t('subjectTaxonRestriction, owlSomeValuesFrom, 'subjectTaxon),
        t('annotation, associationHasObject / rdfType / owlIntersectionOf / rdfRest.* / rdfFirst, 'object),
        t('annotation, associationHasObject / rdfType / owlIntersectionOf / rdfRest.* / rdfFirst, 'objectTaxonRestriction),
        t('objectTaxonRestriction, owlOnProperty, in_taxon),
        t('objectTaxonRestriction, owlSomeValuesFrom, 'objectTaxon),
        t('annotation, associationIsNegated, 'negated),
        t('annotation, has_evidence, 'evidence),
        t('evidence, dcSource, 'source),
        t('evidence, rdfType, 'evidenceType),
        t('evidenceType, rdfsIsDefinedBy, ECO)),
        new ElementFilter(new E_IsIRI(new ExprVar("subject"))),
        new ElementFilter(new E_IsIRI(new ExprVar("object"))),
        new ElementFilter(new E_LogicalOr(new E_Equals(new ExprVar("subject"), new NodeValueNode(term)), new E_Equals(new ExprVar("object"), new NodeValueNode(term)))),
        optional(bgp(
          t('subjectTaxon, owlEquivalentClass, 'subjectVTO),
          t('subjectVTO, rdfsIsDefinedBy, VTO)),
          new ElementFilter(new E_IsIRI(new ExprVar("subjectVTO")))),
          optional(bgp(
            t('objectTaxon, owlEquivalentClass, 'objectVTO),
            t('objectVTO, rdfsIsDefinedBy, VTO)),
            new ElementFilter(new E_IsIRI(new ExprVar("objectVTO")))))
    val relationVar = Var.alloc("relation")
    query.setValuesDataBlock(List(relationVar).asJava, List(
      BindingFactory.binding(relationVar, NodeFactory.createURI(homologous_to.getIRI.toString)),
      BindingFactory.binding(relationVar, NodeFactory.createURI(serially_homologous_to.getIRI.toString))).asJava)
    query
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