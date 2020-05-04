package org.phenoscape.kb.util

import scala.concurrent.Await
import scala.concurrent.duration._
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.QuerySolution
import org.apache.jena.sparql.syntax.Element
import org.apache.jena.sparql.syntax.ElementGroup
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.RDFS
import org.phenoscape.kb.KBVocab.KBMainGraph
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.sparql.SPARQLInterpolation._
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
import org.phenoscape.kb.App

object SPARQLEntityChecker extends OWLEntityChecker {

  private val factory = OWLManager.getOWLDataFactory

  def getOWLAnnotationProperty(label: String): OWLAnnotationProperty =
    queryResult(
      label,
      OWLRDFVocabulary.OWL_ANNOTATION_PROPERTY.getIRI,
      factory.getOWLAnnotationProperty
    )

  def getOWLClass(label: String): OWLClass =
    queryResult(label, OWLRDFVocabulary.OWL_CLASS.getIRI, factory.getOWLClass)

  def getOWLDataProperty(label: String): OWLDataProperty =
    queryResult(
      label,
      OWLRDFVocabulary.OWL_DATA_PROPERTY.getIRI,
      factory.getOWLDataProperty
    )

  def getOWLDatatype(label: String): OWLDatatype =
    queryResult(
      label,
      OWLRDFVocabulary.OWL_DATATYPE.getIRI,
      factory.getOWLDatatype
    )

  def getOWLIndividual(label: String): OWLNamedIndividual =
    queryResult(
      label,
      OWLRDFVocabulary.OWL_NAMED_INDIVIDUAL.getIRI,
      factory.getOWLNamedIndividual
    )

  def getOWLObjectProperty(label: String): OWLObjectProperty =
    queryResult(
      label,
      OWLRDFVocabulary.OWL_OBJECT_PROPERTY.getIRI,
      factory.getOWLObjectProperty
    )

  private def buildQuery(label: String, entityType: IRI): String = {
    val literalType = NodeFactory.createURI(XSDDatatype.XSDstring.getURI)
    sparql"""
        SELECT DISTINCT ?iri FROM $KBMainGraph
        WHERE {
          ?iri ${RDFS.label} $label^^$literalType .
          ?iri ${RDF.`type`} $entityType .
          FILTER NOT EXISTS { ?iri ${RDFS.isDefinedBy} <http://purl.obolibrary.org/obo/ncbitaxon.owl> }
        } LIMIT 1
      """.text
  }

  private def resultFrom[T](func: IRI => T): QuerySolution => T =
    (qs: QuerySolution) => func(IRI.create(qs.getResource("iri").getURI))

  private def queryResult[T >: Null](
      label: String,
      entityType: IRI,
      entityConstructor: IRI => T
  ): T = {
    val queryLabel =
      if (label.startsWith("'") && label.endsWith("'"))
        label.drop(1).dropRight(1)
      else label
    val query = buildQuery(queryLabel, entityType)
    Await
      .result(
        App.executeSPARQLQueryString(query, resultFrom(entityConstructor)),
        60.seconds
      )
      .headOption
      .orNull
  }

  private def triplesBlock(elements: Element*): ElementGroup = {
    val block = new ElementGroup()
    elements.foreach(block.addElement)
    block
  }

}
