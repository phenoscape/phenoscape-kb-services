package org.phenoscape.kb

import org.apache.jena.vocabulary.OWL2
import org.apache.jena.vocabulary.RDFS
import org.phenoscape.scowl._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_LABEL
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_SUBCLASS_OF
import org.apache.jena.vocabulary.RDF

object KBVocab {

  val BDSearchPrefix = "http://www.bigdata.com/rdf/search#"
  val BDSearch = IRI.create(s"${BDSearchPrefix}search")
  val BDMatchAllTerms = IRI.create(s"${BDSearchPrefix}matchAllTerms")
  val BDRank = IRI.create(s"${BDSearchPrefix}rank")
  val rdfsLabel = ObjectProperty(RDFS_LABEL.getIRI)
  val rdfsSubClassOf = ObjectProperty(RDFS_SUBCLASS_OF.getIRI)
  val owlIntersectionOf = ObjectProperty(OWL2.intersectionOf.getURI)
  val rdfFirst = ObjectProperty(RDF.first.getURI)
  val rdfRest = ObjectProperty(RDF.rest.getURI)
  val hasExactSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasExactSynonym")
  val hasRelatedSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym")
  val hasNarrowSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym")
  val hasBroadSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym")
  val hasAnnotation = ObjectProperty("http://example.org/hasAnnotation")
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
  val owlNothing = OWLManager.getOWLDataFactory.getOWLNothing
  val Uberon = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")
  val VTO = IRI.create("http://purl.obolibrary.org/obo/vto.owl")
  val PATO = IRI.create("http://purl.obolibrary.org/obo/pato.owl")
  val homologous_to = ObjectProperty("http://purl.obolibrary.org/obo/RO_HOM0000007")
  val serially_homologous_to = ObjectProperty("http://purl.obolibrary.org/obo/RO_HOM0000027")
  val dc_source = ObjectProperty("http://purl.org/dc/elements/1.1/source")
  val oban = "http://purl.org/oban/"
  val association = IRI.create(s"${oban}association")
  val associationHasSubject = ObjectProperty(s"${oban}association_has_subject")
  val associationHasPredicate = ObjectProperty(s"${oban}association_has_predicate")
  val associationHasObject = ObjectProperty(s"${oban}association_has_object")
  // this 'negated' property is not part of OBAN, so we use a phenoscape namespace
  val associationIsNegated =  IRI.create("http://purl.org/phenoscape/oban/is_negated")

  val has_phenotype = IRI.create("http://purl.obolibrary.org/obo/RO_0002200")

  val KBMainGraph = IRI.create("http://kb.phenoscape.org/")
  val KBClosureGraph = IRI.create("http://kb.phenoscape.org/closure")

  val entityRoot = IRI.create("http://purl.obolibrary.org/obo/UBERON_0000061")
  val qualityRoot = IRI.create("http://purl.obolibrary.org/obo/PATO_0000001")
  val taxonRoot = IRI.create("http://purl.obolibrary.org/obo/VTO_0000001")

  val InferredPresence = IRI.create("http://purl.org/phenoscape/vocab.owl#inferred_presence")
  val InferredAbsence = IRI.create("http://purl.org/phenoscape/vocab.owl#inferred_absence")

  val AnnotatedGene = IRI.create("http://purl.org/phenoscape/vocab.owl#AnnotatedGene")
  val Pseudogene = IRI.create("http://purl.obolibrary.org/obo/SO_0000336")
  val ProteinCodingGene = IRI.create("http://purl.obolibrary.org/obo/SO_0001217")
  val lincRNA_gene = IRI.create("http://purl.obolibrary.org/obo/SO_0001641")
  val lncRNA_gene = IRI.create("http://purl.obolibrary.org/obo/SO_0002127")
  val miRNA_gene = IRI.create("http://purl.obolibrary.org/obo/SO_0001265")

}
