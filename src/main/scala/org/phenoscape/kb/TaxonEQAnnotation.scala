package org.phenoscape.kb

import org.phenoscape.owl.Vocab._
import org.phenoscape.scowl._

object TaxonEQAnnotation {

  val ps_entity_term = ObjectProperty(entity_term.getIRI)
  val ps_quality_term = ObjectProperty(quality_term.getIRI)
  val ps_related_entity_term = ObjectProperty(related_entity_term.getIRI)

  //  private def buildQuery(entity: OWLClassExpression = owlThing, taxon: OWLClassExpression = owlThing, publications: Iterable[IRI] = Nil): Query = {
  //    val entityPatterns = if (entity == owlThing) Nil else
  //      t('phenotype, ps_entity_term | ps_related_entity_term, 'entity) :: t('entity, rdfsSubClassOf, entity.asOMN) :: Nil
  //    val filters = if (publications.isEmpty) Nil else
  //      new ElementFilter(new E_OneOf(new ExprVar('matrix), new ExprList(publications.map(new NodeValueNode(_)).toList))) :: Nil
  //    select_distinct() from "http://kb.phenoscape.org/" where (
  //      bgp(
  //        t('state, dcDescription, 'state_desc) ::
  //          t('state, describes_phenotype, 'phenotype) ::
  //          t('phenotype, ps_entity_term, 'annotated_entity) ::
  //          t('phenotype, ps_quality_term, 'annotated_quality) ::
  //          t('matrix, has_character / may_have_state_value, 'state) ::
  //          t('matrix, rdfsLabel, 'matrix_label) ::
  //          t('taxon, exhibits_state, 'state) ::
  //          t('taxon, rdfsLabel, 'taxon_label) ::
  //          entityPatterns: _*) ::
  //        optional(bgp(t('phenotype, ps_related_entity_term, 'annotated_related_entity))) ::
  //        filters: _*)
  //  }

}
