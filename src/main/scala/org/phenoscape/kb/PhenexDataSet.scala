package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.phenoscape.io.NeXMLWriter
import org.phenoscape.model.DataSet

import java.io.StringWriter
import java.util.UUID

object PhenexDataSet {

  implicit val DataSetMarshaller: ToEntityMarshaller[DataSet] =
    Marshaller.stringMarshaller(MediaTypes.`application/xml`).compose { dataset =>
      val writer = new NeXMLWriter("c" + UUID.randomUUID.toString)
      writer.setDataSet(dataset)
      val output = new StringWriter()
      writer.write(output)
      output.toString
    }

}
