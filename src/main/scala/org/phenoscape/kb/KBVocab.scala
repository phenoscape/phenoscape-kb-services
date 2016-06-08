package org.phenoscape.kb

import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_LABEL
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_SUBCLASS_OF
import com.hp.hpl.jena.vocabulary.OWL2
import com.hp.hpl.jena.vocabulary.RDFS
import org.semanticweb.owlapi.apibinding.OWLManager

object KBVocab {

  val BDSearchPrefix = "http://www.bigdata.com/rdf/search#"
  val BDSearch = IRI.create(s"${BDSearchPrefix}search")
  val BDMatchAllTerms = IRI.create(s"${BDSearchPrefix}matchAllTerms")
  val BDRank = IRI.create(s"${BDSearchPrefix}rank")
  val rdfsLabel = ObjectProperty(RDFS_LABEL.getIRI)
  val rdfsSubClassOf = ObjectProperty(RDFS_SUBCLASS_OF.getIRI)
  val hasExactSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasExactSynonym")
  val hasRelatedSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym")
  val definition = ObjectProperty("http://purl.obolibrary.org/obo/IAO_0000115")
  val owlAnnotatedSource = IRI.create(OWL2.annotatedSource.getURI)
  val owlAnnotatedProperty = IRI.create(OWL2.annotatedProperty.getURI)
  val owlAnnotatedTarget = IRI.create(OWL2.annotatedTarget.getURI)
  val owlOnProperty = IRI.create(OWL2.onProperty.getURI)
  val owlSomeValuesFrom = IRI.create(OWL2.someValuesFrom.getURI)
  val hasSynonymType = IRI.create("http://www.geneontology.org/formats/oboInOwl#hasSynonymType")
  val CommonNameSynonymType = IRI.create("http://purl.obolibrary.org/obo/vto#COMMONNAME")
  val rdfsIsDefinedBy = ObjectProperty(RDFS.isDefinedBy.getURI)
  val owlClass = IRI.create(OWL2.Class.getURI)
  val owlNamedIndividual = IRI.create(OWL2.NamedIndividual.getURI)
  val owlEquivalentClass = ObjectProperty(OWL2.equivalentClass.getURI)
  val owlDeprecated = ObjectProperty(OWL2.deprecated.getURI)
  val owlThing = OWLManager.getOWLDataFactory.getOWLThing
  val Uberon = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")
  val VTO = IRI.create("http://purl.obolibrary.org/obo/vto.owl")
  val PATO = IRI.create("http://purl.obolibrary.org/obo/pato.owl")

  val KBMainGraph = "http://kb.phenoscape.org/"
  val KBClosureGraph = "http://kb.phenoscape.org/closure"

}