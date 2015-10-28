package org.phenoscape.kb

import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.scowl.OWL._
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.semanticweb.owlapi.model.OWLClassExpression
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import org.semanticweb.owlapi.model.IRI
import scala.concurrent.Future

object Study {

  val AnatomicalEntity = Class(ANATOMICAL_ENTITY)
  val Chordata = Class(CHORDATA)

  def queryStudies(entityOpt: Option[OWLClassExpression], taxonOpt: Option[OWLClassExpression]): Future[Seq[MinimalTerm]] = for {
    query <- App.expandWithOwlet(buildQuery(entityOpt, taxonOpt))
    studies <- App.executeSPARQLQuery(query, queryToTerm)
  } yield {
    studies
  }

  def buildQuery(entityOpt: Option[OWLClassExpression], taxonOpt: Option[OWLClassExpression]): Query = {
    val entityPatterns = entityOpt.toList.flatMap(entity =>
      t('study, has_character / may_have_state_value / describes_phenotype, 'phenotype) ::
        t('phenotype, rdfsSubClassOf, (phenotype_of some entity).asOMN) :: Nil)
    val taxonPatterns = taxonOpt.toList.flatMap(taxon =>
      t('study, has_TU / has_external_reference, 'taxon) ::
        t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil)
    val query = select_distinct('study, 'study_label) where (
      bgp(
        (t('study, rdfType, CharacterStateDataMatrix) ::
          t('study, rdfsLabel, 'study_label) ::
          entityPatterns ++ taxonPatterns): _*))

    query.addOrderBy('study_label)
    query.addOrderBy('study)
    query
  }

  private def queryToTerm(result: QuerySolution): MinimalTerm =
    MinimalTerm(IRI.create(result.getResource("study").getURI),
      result.getLiteral("study_label").getLexicalForm)

}