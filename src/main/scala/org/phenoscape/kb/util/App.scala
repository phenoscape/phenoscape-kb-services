package org.phenoscape.kb.util

import org.semanticweb.owlapi.reasoner.OWLReasoner
import org.semanticweb.owlapi.apibinding.OWLManager
import java.io.File
import org.semanticweb.elk.owlapi.ElkReasonerFactory
import org.semanticweb.owlapi.reasoner.InferenceType
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.server.spi.ContainerLifecycleListener
import org.glassfish.jersey.server.spi.Container
import javax.ws.rs.ext.Provider

/**
 * Caution—this object must be safe to access from multiple threads.
 */
@Provider
object App extends ContainerLifecycleListener {

  val KB_SPARQL_ENDPOINT_PROPERTY = "org.phenoscape.kb.endpoint"
  val KB_ONTOLOGY_FILE_PROPERTY = "org.phenoscape.kb.owl.file"

  val reasoner: OWLReasoner = new ElkReasonerFactory().createReasoner(
    OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(new File(System.getProperty(KB_ONTOLOGY_FILE_PROPERTY))))
  reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY)

  val endpoint: String = System.getProperty(KB_SPARQL_ENDPOINT_PROPERTY)

  override def onStartup(container: Container): Unit = {}

  override def onReload(container: Container): Unit = { println("Disposing"); reasoner.dispose() }

  override def onShutdown(container: Container): Unit = { println("Disposing"); reasoner.dispose() }

}
