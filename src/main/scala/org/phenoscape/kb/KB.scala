package org.phenoscape.kb

import org.phenoscape.kb.Main.system.dispatcher
import scala.concurrent.Future
import org.phenoscape.owl.Vocab._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.owlet.SPARQLComposer._
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct
import com.hp.hpl.jena.sparql.syntax.ElementNotExists
import com.hp.hpl.jena.vocabulary.OWL2
import org.phenoscape.scowl.OWL._
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import com.hp.hpl.jena.sparql.expr.E_NotExists
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.sparql.core.TriplePath
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.expr.E_Exists
import spray.json.DefaultJsonProtocol._
import spray.json._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._
import spray.http._
import spray.httpx._
import org.phenoscape.kb.KBVocab._

object KB {

  val rdfsLabel = org.phenoscape.kb.KBVocab.rdfsLabel

  def annotationSummary: Future[KBAnnotationSummary] = {
    val matrices = annotatedMatrixCount
    val taxa = annotatedTaxonCount
    val characters = annotatedCharacterCount
    val states = annotatedStateCount
    for {
      matrixCount <- matrices
      taxonCount <- taxa
      characterCount <- characters
      stateCount <- states
    } yield KBAnnotationSummary(matrixCount, taxonCount, characterCount, stateCount)
  }

  def annotationReport: Future[String] = {
    val queryFromText = QueryFactory.create("""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ps: <http://purl.org/phenoscape/vocab.owl#>
PREFIX has_character: <http://purl.obolibrary.org/obo/CDAO_0000142>
PREFIX has_state: <http://purl.obolibrary.org/obo/CDAO_0000184>
PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT (STR(?matrix_label) AS ?matrix_file) (STR(?char_number) AS ?character_number) (STR(?char_label) AS ?character_text) (STR(?symbol) AS ?state_symbol) (STR(?state_label) AS ?state_text) (STR(?entity) AS ?entity_id) ?entity_name (STR(?quality) AS ?quality_id) ?quality_name (STR(?related_entity) AS ?related_entity_id) ?related_entity_name (GROUP_CONCAT(DISTINCT ?attribute_label; separator=", ") AS ?attributes)
FROM <http://kb.phenoscape.org/>
WHERE
{ 
?state rdfs:label ?state_label .
?matrix has_character: ?matrix_char .
?matrix_char rdfs:label ?char_label .
?matrix_char ps:list_index ?char_number .
?matrix rdfs:label ?matrix_label .
?matrix_char ps:may_have_state_value ?state .
?state ps:state_symbol ?symbol .
?state ps:describes_phenotype ?phenotypeGroup .
?phenotypeGroup rdfs:subClassOf ?phenotype .
?phenotype ps:entity_term ?entity . 
OPTIONAL {   
?entity rdfs:label ?entity_label .
}
?phenotype ps:quality_term ?quality .
OPTIONAL {
?quality rdfs:label ?quality_label .
}
OPTIONAL {
  ?phenotype ps:related_entity_term ?related_entity .
    OPTIONAL {
  ?related_entity rdfs:label ?related_entity_label .
  }
}
OPTIONAL {
  ?attribute oboInOwl:inSubset <http://purl.obolibrary.org/obo/TEMP#character_slim> .
  ?quality rdfs:subClassOf* ?attribute .
  ?attribute rdfs:label ?attribute_label .
}
  BIND(COALESCE(STR(?entity_label), "") AS ?entity_name)
  BIND(COALESCE(STR(?quality_label), "") AS ?quality_name)
  BIND(COALESCE(STR(?related_entity_label), "") AS ?related_entity_name)
}
GROUP BY ?matrix_label ?char_number ?char_label ?symbol ?state_label ?entity ?entity_name ?quality ?quality_name ?related_entity ?related_entity_name      
      """)
    App.executeSPARQLQuery(queryFromText).map(App.resultSetToTSV)
  }

  def characterCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('character, rdfType, StandardCharacter)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("character"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def stateCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, rdfType, StandardState)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def annotatedCharacterCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('character, rdfType, StandardCharacter),
        t('character, may_have_state_value, 'state),
        t('state, describes_phenotype, 'phenotype)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("character"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def annotatedStateCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, rdfType, StandardState),
        t('state, describes_phenotype, 'phenotype)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def taxonCount: Future[Int] = {
    import org.phenoscape.owl.Vocab.Taxon
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, rdfsIsDefinedBy, VTO)),
        new ElementFilter(new E_NotExists(triplesBlock(bgp(t('taxon, owlDeprecated, "true" ^^ XSDDatatype.XSDboolean))))))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  private def triplesBlock(elements: Element*): ElementGroup = {
    val block = new ElementGroup()
    elements.foreach(block.addElement)
    block
  }

  def annotatedTaxonCount: Future[Int] = {
    import org.phenoscape.owl.Vocab.Taxon
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, exhibits_state / describes_phenotype, 'phenotype)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def matrixCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('matrix, rdfType, CharacterStateDataMatrix)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("matrix"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def annotatedMatrixCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('matrix, rdfType, CharacterStateDataMatrix)),
        new ElementFilter(new E_Exists(triplesBlock(bgp(t('matrix, has_character / may_have_state_value / describes_phenotype, 'phenotype))))))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("matrix"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

}

case class KBAnnotationSummary(annotatedMatrices: Int, annotatedTaxa: Int, annotatedCharacters: Int, annotatedStates: Int) {

  def toJSON: JsObject = {
    Map("annotated_matrices" -> annotatedMatrices,
      "annotated_taxa" -> annotatedTaxa,
      "annotated_characters" -> annotatedCharacters,
      "annotated_states" -> annotatedStates).toJson.asJsObject
  }

}

object KBAnnotationSummary {

  implicit val KBAnnotationSummaryMarshaller = Marshaller.delegate[KBAnnotationSummary, JsObject](MediaTypes.`application/json`) { result =>
    result.toJSON
  }

}
