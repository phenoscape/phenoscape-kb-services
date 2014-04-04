package org.phenoscape.kb.resource

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.Entity
import javax.ws.rs.core.Form
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import org.phenoscape.owl.Vocab._
import org.semanticweb.owlapi.model.OWLEntity
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary

@Path("contents/mod")
class KBMODContent {

	
	

  @GET
  @Produces(Array("text/tab-separated-values"))
  def urlQuery(): Response = {
    ???
  }

  private def §(entity: OWLEntity): String = §(entity.getIRI)
  private def §(iri: IRI): String = s"<${iri.toString}>"
  
  object sparql {
    
    def apply(patterns: (AnyRef, AnyRef, AnyRef)*): String = ???
  }
  
  private val sourcesQuery1 = sparql (
    ('annotation, rdfType, AnnotatedPhenotype),
    ('annotation, associated_with_taxon, 'taxon)
  )
  
  val rdfType = OWLRDFVocabulary.RDF_TYPE.getIRI
  
  private def pattern(items: (AnyRef, AnyRef, AnyRef)*): String = {
    val a = items.head
    
    ???
  }
  
    private val sourcesQuery = s"""
SELECT (COUNT(DISTINCT ?source) AS ?count)
FROM <http://kb.phenoscape.org/>
WHERE
{
  {
  ?annotation ${§(rdfType)} ${§(AnnotatedPhenotype)} .
  }
  UNION
  {
  ?annotation ${§(rdfType)} ${§(GeneExpression)} .
  }
?annotation ${§(associated_with_taxon)} ?taxon .
?annotation ${§(associated_with_gene)} ?gene .
?annotation ${§(dcSource)} ?source .
}
"""

    private val phenotypesQuery = s"""
SELECT (COUNT(DISTINCT ?annotation) AS ?count)
FROM <http://kb.phenoscape.org/>
WHERE
{
?annotation ${§(rdfType)} <${§(AnnotatedPhenotype)}> .  
?annotation <${§(associated_with_taxon)}> ?taxon .
?annotation <${§(associated_with_gene)}> ?gene .
}
"""

    private val expressionQuery = s"""
SELECT (COUNT(DISTINCT ?annotation) AS ?count)
FROM <http://kb.phenoscape.org/>
WHERE
{
?annotation ${§(rdfType)} <${§(GeneExpression)}> .  
?annotation <${§(associated_with_taxon)}> ?taxon .
?annotation <${§(associated_with_gene)}> ?gene .
}
"""

}