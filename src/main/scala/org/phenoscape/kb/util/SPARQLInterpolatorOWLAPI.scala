package org.phenoscape.kb.util

import contextual._
import org.apache.jena.query.ParameterizedSparqlString
import org.phenoscape.sparql.SPARQLInterpolation.SPARQLInterpolator.SPARQLContext
import org.phenoscape.sparql.SPARQLInterpolation._
import org.semanticweb.owlapi.model.{
  IRI,
  OWLAnnotationProperty,
  OWLClass,
  OWLObjectProperty
}
import scalaz._

object SPARQLInterpolatorOWLAPI {

  implicit val embedIRIInSPARQL =
    SPARQLInterpolator.embed[IRI](Case(SPARQLContext, SPARQLContext)(iri => {
      val pss = new ParameterizedSparqlString()
      pss.appendIri(iri.toString)
      pss.toString
    }))

  implicit val embedOWLClassInSPARQL = SPARQLInterpolator.embed[OWLClass](
    Case(SPARQLContext, SPARQLContext)(obj => {
      val pss = new ParameterizedSparqlString()
      pss.appendIri(obj.getIRI.toString)
      pss.toString
    })
  )

  implicit val embedOWLObjectPropertyInSPARQL = SPARQLInterpolator
    .embed[OWLObjectProperty](Case(SPARQLContext, SPARQLContext)(obj => {
      val pss = new ParameterizedSparqlString()
      pss.appendIri(obj.getIRI.toString)
      pss.toString
    }))

  implicit val embedOWLAnnotationPropertyInSPARQL = SPARQLInterpolator
    .embed[OWLAnnotationProperty](Case(SPARQLContext, SPARQLContext)(obj => {
      val pss = new ParameterizedSparqlString()
      pss.appendIri(obj.getIRI.toString)
      pss.toString
    }))

  implicit val embedSubqueryReferenceInSPARQL =
    SPARQLInterpolator.embed[BlazegraphNamedSubquery](
      Case(SPARQLContext, SPARQLContext)(q => s"INCLUDE %${q.ids.min}")
    )

  implicit val sparqlSemigroup: Semigroup[QueryText] =
    Semigroup.instance((a, b) => QueryText(a.text + b.text))

}
