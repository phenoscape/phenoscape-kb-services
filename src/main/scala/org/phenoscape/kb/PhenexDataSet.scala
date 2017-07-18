package org.phenoscape.kb

import java.io.StringWriter
import java.util.UUID

import org.phenoscape.io.NeXMLWriter
import org.phenoscape.model.DataSet

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model.MediaTypes

object PhenexDataSet {

  implicit val DataSetMarshaller: ToEntityMarshaller[DataSet] = Marshaller.stringMarshaller(MediaTypes.`application/xml`).compose { dataset =>
    val writer = new NeXMLWriter(UUID.randomUUID.toString)
    writer.setDataSet(dataset)
    val output = new StringWriter()
    writer.write(output)
    output.toString
  }

}