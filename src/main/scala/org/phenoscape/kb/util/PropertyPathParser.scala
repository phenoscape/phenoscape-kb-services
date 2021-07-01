package org.phenoscape.kb.util

import org.apache.jena.query.{QueryFactory, QueryParseException}
import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.path.Path
import org.apache.jena.sparql.syntax.{ElementPathBlock, ElementVisitorBase, ElementWalker, RecursiveElementVisitor}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object PropertyPathParser {

  private def queryShell(path: String): String = s"SELECT * WHERE { ?s $path ?o .}"

  def parsePropertyPath(path: String): Try[Path] = {
    val queryText = queryShell(path)
    Try(QueryFactory.create(queryText)).flatMap { query =>
      val visitor = new TriplePathVisitor()
      ElementWalker.walk(query.getQueryPattern, visitor)
      visitor.triplePath match {
        case Some(path) => Success(path.getPath)
        case None       => Failure(new IllegalArgumentException("Unable to parse property path"))
      }
    }

  }

  private class TriplePathVisitor extends RecursiveElementVisitor(new ElementVisitorBase()) {

    var triplePath: Option[TriplePath] = None

    override def visit(el: ElementPathBlock): Unit =
      triplePath = el.getPattern.getList.asScala.headOption

  }

}
