package org.phenoscape.kb

import scala.concurrent.Future

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.E_Exists
import org.apache.jena.sparql.expr.E_NotExists
import org.apache.jena.sparql.expr.ExprVar
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct
import org.apache.jena.sparql.syntax.Element
import org.apache.jena.sparql.syntax.ElementFilter
import org.apache.jena.sparql.syntax.ElementGroup
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import spray.json._
import spray.json.DefaultJsonProtocol._
import java.util.Date
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.phenoscape.sparql.SPARQLInterpolation.QueryText
import org.openrdf.model.vocabulary.DCTERMS
import org.apache.jena.vocabulary.DCTerms
import org.semanticweb.owlapi.model.IRI
import java.time.Instant

object KB {

  val rdfsLabel = org.phenoscape.kb.KBVocab.rdfsLabel

  def annotationSummary: Future[KBAnnotationSummary] = {
    val matrices = annotatedMatrixCount
    val taxa = annotatedTaxonCount
    val characters = annotatedCharacterCount
    val states = annotatedStateCount
    val built = buildDate
    for {
      builtTime <- built
      matrixCount <- matrices
      taxonCount <- taxa
      characterCount <- characters
      stateCount <- states
    } yield KBAnnotationSummary(builtTime, matrixCount, taxonCount, characterCount, stateCount)
  }

  def annotationReport: Future[String] = {
    val query =
      sparql"""
          PREFIX ps: <http://purl.org/phenoscape/vocab.owl#>
          PREFIX has_character: <http://purl.obolibrary.org/obo/CDAO_0000142>
          PREFIX has_state: <http://purl.obolibrary.org/obo/CDAO_0000184>
          PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
          PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

         SELECT (?matrix_label AS ?matrix_file) (?char_number AS ?character_number) (?char_label AS ?character_text) (?symbol AS ?state_symbol) (?state_label AS ?state_text) (?entity AS ?entity_id) ?entity_name (?quality AS ?quality_id) ?quality_name (?related_entity AS ?related_entity_id) ?related_entity_name ?attributes

         FROM $KBMainGraph
          
          WITH {
            SELECT ?quality (GROUP_CONCAT(DISTINCT ?attribute_label; separator=", ") AS ?attributes) WHERE {
              ?attribute oboInOwl:inSubset <http://purl.obolibrary.org/obo/TEMP#character_slim> .
              ?quality ${KBVocab.rdfsSubClassOf}* ?attribute .
              ?attribute $rdfsLabel ?attribute_label .
           } GROUP BY ?quality 
          } AS %attributes
       
        WHERE
        { 
          ?state $rdfsLabel ?state_label .
          ?matrix has_character: ?matrix_char .
          ?matrix_char $rdfsLabel ?char_label .
          ?matrix_char ps:list_index ?char_number .
          ?matrix $rdfsLabel ?matrix_label .
          ?matrix_char ps:may_have_state_value ?state .
          ?state ps:state_symbol ?symbol .
          ?state ps:describes_phenotype ?phenotype .
          ?phenotype ps:entity_term ?entity . 
          
          OPTIONAL {   
          ?entity $rdfsLabel ?entity_label .
          }
          ?phenotype ps:quality_term ?quality .
          OPTIONAL {
          ?quality $rdfsLabel ?quality_label .
          }
          OPTIONAL {
            ?phenotype ps:related_entity_term ?related_entity .
              OPTIONAL {
            ?related_entity $rdfsLabel ?related_entity_label .
            }
          }
          OPTIONAL {
            INCLUDE %attributes
          }
            
          BIND(COALESCE(STR(?entity_label), "") AS ?entity_name)
          BIND(COALESCE(STR(?quality_label), "") AS ?quality_name)
          BIND(COALESCE(STR(?related_entity_label), "") AS ?related_entity_name)
        }
              """
    App.executeSPARQLQuery(query.toQuery).map(App.resultSetToTSV)
  }

  def characterCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (bgp(t('character, rdfType, StandardCharacter)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("character"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def stateCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (bgp(t('state, rdfType, StandardState)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def annotatedCharacterCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (bgp(t('character, rdfType, StandardCharacter),
                                                                     t('character, may_have_state_value, 'state),
                                                                     t('state, describes_phenotype, 'phenotype)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("character"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def annotatedStateCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (bgp(t('state, rdfType, StandardState),
                                                                     t('state, describes_phenotype, 'phenotype)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def taxonCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (bgp(t('taxon, rdfsIsDefinedBy, VTO)),
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
    val query =
      select() from "http://kb.phenoscape.org/" where (bgp(t('taxon, exhibits_state / describes_phenotype, 'phenotype)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def matrixCount: Future[Int] = {
    val query = select() from "http://kb.phenoscape.org/" where (bgp(t('matrix, rdfType, CharacterStateDataMatrix)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("matrix"))))
    App.executeSPARQLQuery(query).map(ResultCount.count)
  }

  def annotatedMatrixCount: Future[Int] = {
    val query =
      sparql"""
              SELECT (COUNT(DISTINCT ?matrix) AS ?count)
              FROM $KBMainGraph
              WHERE {
                ?matrix $rdfType $CharacterStateDataMatrix
                FILTER EXISTS {?matrix $has_character / $may_have_state_value / $describes_phenotype ?phenotype}
              }
              """
    App.executeSPARQLQuery(query.toQuery).map(ResultCount.count)
  }

  def buildDate: Future[Instant] = {
    val query = sparql"""
      SELECT ?date
      FROM $KBMainGraph
      WHERE {
        $KBMainGraph ${DCTerms.created} ?date . 
      }
      """
    App.executeSPARQLQueryString(query.text, res => Instant.parse(res.getLiteral("date").getLexicalForm)).map(_.head)
  }

  def getKBMetadata: Future[KBMetadata] = {
    val builtFut = buildDate
    val ontologiesFut = kbOntologies
    for {
      built <- builtFut
      ontologies <- ontologiesFut
    } yield KBMetadata(built, ontologies)
  }

  def kbOntologies: Future[Set[(IRI, IRI)]] = {
    val query = sparql"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?ont ?version
    FROM $KBMainGraph
    WHERE {
      ?ont $rdfType owl:Ontology .
  	  ?ont owl:versionIRI ?version
    }
  """

    App
      .executeSPARQLQueryString(
        query.text,
        qs => (IRI.create(qs.getResource("ont").getURI), IRI.create(qs.getResource("version").getURI)))
      .map(_.toSet)
  }

}

case class KBMetadata(built: Instant, ontologies: Set[(IRI, IRI)]) extends JSONResultItem {

  def toJSON: JsObject =
    Map(
      "build_time" -> built.toString.toJson,
      "ontologies" -> ontologies.toSeq
        .sortBy(_.toString)
        .map {
          case (ont, version) =>
            JsObject(
              "@id" -> ont.toString.toJson,
              "version" -> version.toString.toJson
            ).toJson
        }
        .toJson
    ).toJson.asJsObject

}

case class KBAnnotationSummary(built: Instant,
                               annotatedMatrices: Int,
                               annotatedTaxa: Int,
                               annotatedCharacters: Int,
                               annotatedStates: Int) {

  def toJSON: JsObject =
    Map(
      "build_time" -> built.toString.toJson,
      "annotated_matrices" -> annotatedMatrices.toJson,
      "annotated_taxa" -> annotatedTaxa.toJson,
      "annotated_characters" -> annotatedCharacters.toJson,
      "annotated_states" -> annotatedStates.toJson
    ).toJson.asJsObject

}

object KBAnnotationSummary {

  implicit val KBAnnotationSummaryMarshaller: ToEntityMarshaller[KBAnnotationSummary] =
    Marshaller.combined(result => result.toJSON)

}
