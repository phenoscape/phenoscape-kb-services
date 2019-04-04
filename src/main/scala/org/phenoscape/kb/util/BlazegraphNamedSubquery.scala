package org.phenoscape.kb.util

import java.util.UUID

import org.phenoscape.sparql.SPARQLInterpolation.{QueryText, _}

final case class BlazegraphNamedSubquery(queryText: QueryText, ids: Set[String]) {

  def namedQuery: QueryText = sparql" WITH { $queryText } AS %${QueryText(ids.min)} "

  def updateReferences(queryString: String): String = ids.foldLeft(queryString)((currentQuery, id) => currentQuery.replaceAllLiterally(id, ids.min))

}

object BlazegraphNamedSubquery {

  def apply(queryText: QueryText): BlazegraphNamedSubquery = BlazegraphNamedSubquery(queryText, Set(UUID.randomUUID.toString.replaceAllLiterally("-", "")))

  def unifyQueries(queries: Set[BlazegraphNamedSubquery]): Set[BlazegraphNamedSubquery] = if (queries.nonEmpty)
    queries.groupBy(_.queryText).values.map(_.reduce { (a, b) => BlazegraphNamedSubquery(a.queryText, a.ids ++ b.ids)
    }).toSet
  else queries

  def updateReferencesFor(subqueries: Set[BlazegraphNamedSubquery], queryString: String): String =
    subqueries.foldLeft(queryString)((currentQuery, subquery) => subquery.updateReferences(currentQuery))

}
