package org.phenoscape.kb.queries

import org.phenoscape.kb.KBVocab
import org.semanticweb.owlapi.model.IRI

object QueryUtil {

  sealed trait QualitySpec {

    def asOptionalQuality: Option[IRI] = None

  }

  object QualitySpec {

    def fromIRI(iri: IRI): QualitySpec =
      iri match {
        case KBVocab.InferredPresence => InferredPresence
        case KBVocab.InferredAbsence  => InferredAbsence
        case other                    => PhenotypicQuality(Some(other))
      }

  }

  final case class PhenotypicQuality(quality: Option[IRI]) extends QualitySpec {

    override def asOptionalQuality: Option[IRI] = quality

  }

  final case object InferredPresence extends QualitySpec

  final case object InferredAbsence extends QualitySpec

}
