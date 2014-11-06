package org.phenoscape.kb

import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDFS_LABEL

import com.hp.hpl.jena.vocabulary.OWL2
import com.hp.hpl.jena.vocabulary.RDFS


object Vocab {
  
   val BDSearchPrefix = "http://www.bigdata.com/rdf/search#"
   val BDSearch = IRI.create(s"${BDSearchPrefix}search")
   val BDMatchAllTerms = IRI.create(s"${BDSearchPrefix}matchAllTerms")
   val BDRank = IRI.create(s"${BDSearchPrefix}rank")
   val rdfsLabel = ObjectProperty(RDFS_LABEL.getIRI)
   val hasExactSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasExactSynonym")
   val hasRelatedSynonym = ObjectProperty("http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym")
   val rdfsIsDefinedBy = ObjectProperty(RDFS.isDefinedBy.getURI)
   val owlClass = IRI.create(OWL2.Class.getURI)
   val Uberon = IRI.create("http://purl.obolibrary.org/obo/uberon.owl")

}