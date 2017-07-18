package org.phenoscape.kb

import org.phenoscape.owlet.ManchesterSyntaxClassExpressionParser
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.semanticweb.owlapi.model.OWLClassExpression

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.unmarshalling.Unmarshaller
import scalaz._

object OWLFormats {

  implicit val ManchesterSyntaxClassExpressionUnmarshaller: Unmarshaller[String, OWLClassExpression] = Unmarshaller.strict { text =>
    ManchesterSyntaxClassExpressionParser.parse(text) match {
      case Success(expression) => expression
      case Failure(message)    => throw new IllegalArgumentException(s"Invalid Manchester syntax: $message")
    }
  }

  implicit val OWLClassExpressionMarshaller: ToEntityMarshaller[OWLClassExpression] = Marshaller.stringMarshaller(MediaTypes.`text/plain`).compose(expression =>
    expression.asOMN.getLiteralLexicalForm)

}