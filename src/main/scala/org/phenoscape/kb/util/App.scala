package org.phenoscape.kb.util

import org.semanticweb.owlapi.reasoner.OWLReasoner
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.File
import org.semanticweb.elk.owlapi.ElkReasonerFactory
import org.semanticweb.owlapi.reasoner.InferenceType

/**
 * Caution—this object must be safe to access from multiple threads.
 */
object App {

  //FIXME set ontology file in Java properties
  val reasoner: OWLReasoner = new ElkReasonerFactory().createReasoner(
    OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(new File("/data/phenoscape-kb/tbox.owl")))
  reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY)

  //FIXME set endpoint address in Java properties
  val endpoint: String = "http://kb-dev.phenoscape.org/bigsparql"

}