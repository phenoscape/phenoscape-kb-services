package org.phenoscape.kb

import scala.concurrent.Await
import scala.concurrent.duration._

import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.expression.OWLEntityChecker
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLAnnotationProperty
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.model.OWLDataProperty
import org.semanticweb.owlapi.model.OWLDatatype
import org.semanticweb.owlapi.model.OWLNamedIndividual
import org.semanticweb.owlapi.model.OWLObjectProperty
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution

object SPARQLEntityChecker extends OWLEntityChecker {

  private val factory = OWLManager.getOWLDataFactory

  def getOWLAnnotationProperty(label: String): OWLAnnotationProperty =
    queryResult(label, OWLRDFVocabulary.OWL_ANNOTATION_PROPERTY.getIRI, factory.getOWLAnnotationProperty)

  def getOWLClass(label: String): OWLClass =
    queryResult(label, OWLRDFVocabulary.OWL_CLASS.getIRI, factory.getOWLClass)

  def getOWLDataProperty(label: String): OWLDataProperty =
    queryResult(label, OWLRDFVocabulary.OWL_DATA_PROPERTY.getIRI, factory.getOWLDataProperty)

  def getOWLDatatype(label: String): OWLDatatype =
    queryResult(label, OWLRDFVocabulary.OWL_DATATYPE.getIRI, factory.getOWLDatatype)

  def getOWLIndividual(label: String): OWLNamedIndividual =
    queryResult(label, OWLRDFVocabulary.OWL_NAMED_INDIVIDUAL.getIRI, factory.getOWLNamedIndividual)

  def getOWLObjectProperty(label: String): OWLObjectProperty =
    queryResult(label, OWLRDFVocabulary.OWL_OBJECT_PROPERTY.getIRI, factory.getOWLObjectProperty)

  private def buildQuery(label: String, entityType: IRI): Query =
    select_distinct('iri) from "http://kb.phenoscape.org/" where (
      bgp(
        t('iri, rdfsLabel, NodeFactory.createLiteral(label, XSDDatatype.XSDstring)),
        t('iri, rdfType, entityType))) limit 1

  private def resultFrom[T](func: IRI => T): QuerySolution => T =
    (qs: QuerySolution) => func(IRI.create(qs.getResource("iri").getURI))

  private def queryResult[T >: Null](label: String, entityType: IRI, entityConstructor: IRI => T): T = {
    val queryLabel = if (label.startsWith("'") && label.endsWith("'")) label.drop(1).dropRight(1)
    else label
    val query = buildQuery(queryLabel, entityType)
    Await.result(App.executeSPARQLQuery(query, resultFrom(entityConstructor)), 60.seconds).headOption.orNull
  }

}