package org.phenoscape.kb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.phenoscape.owl.Vocab._
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

object KB {

  private val owlDeprecated = ObjectProperty(OWL2.deprecated.getURI)

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
        t('taxon, rdfType, Taxon)),
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
        t('taxon, rdfType, Taxon)),
        new ElementFilter(new E_Exists(triplesBlock(bgp(t('taxon, exhibits_state / describes_phenotype, 'phenotype))))),
        new ElementFilter(new E_NotExists(triplesBlock(bgp(t('taxon, owlDeprecated, "true" ^^ XSDDatatype.XSDboolean))))))
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
