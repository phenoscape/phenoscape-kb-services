package org.phenoscape.kb

import scala.Left
import scala.Right
import org.phenoscape.owlet.ManchesterSyntaxClassExpressionParser
import org.semanticweb.owlapi.model.OWLClassExpression
import scalaz._
import spray.httpx.unmarshalling.Deserialized
import spray.httpx.unmarshalling.Deserializer
import spray.httpx.unmarshalling.MalformedContent
import spray.http.MediaTypes
import java.util.UUID
import org.phenoscape.model.DataSet
import java.io.StringWriter
import org.phenoscape.io.NeXMLWriter
import spray.httpx.marshalling.Marshaller
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression

object OWLFormats {

  implicit object ManchesterSyntaxClassExpression extends Deserializer[String, OWLClassExpression] {

    def apply(text: String): Deserialized[OWLClassExpression] = ManchesterSyntaxClassExpressionParser.parse(text) match {
      case Success(expression) => Right(expression)
      case Failure(message)    => Left(MalformedContent(message))
    }

  }

  implicit val OWLClassExpressionMarshaller = Marshaller.delegate[OWLClassExpression, String](MediaTypes.`text/plain`) { expression =>
    expression.asOMN.getLiteralLexicalForm
  }

}