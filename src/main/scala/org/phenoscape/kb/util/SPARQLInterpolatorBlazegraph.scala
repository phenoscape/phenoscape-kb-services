package org.phenoscape.kb.util

import contextual._
import org.phenoscape.sparql.SPARQLInterpolation.SPARQLInterpolator.SPARQLContext
import org.phenoscape.sparql.SPARQLInterpolation._

object SPARQLInterpolatorBlazegraph {

  implicit val embedSubqueryReferenceInSPARQL: SPARQLEmbedder[BlazegraphNamedSubquery] =
    SPARQLInterpolator.embed[BlazegraphNamedSubquery](Case(SPARQLContext, SPARQLContext)(q => s"INCLUDE %${q.ids.min}"))

}
