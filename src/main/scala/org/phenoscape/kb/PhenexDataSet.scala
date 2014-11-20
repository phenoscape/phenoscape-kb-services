package org.phenoscape.kb

import spray.httpx.marshalling._
import org.phenoscape.model.DataSet
import spray.http.MediaTypes
import org.phenoscape.io.NeXMLWriter
import java.util.UUID
import java.io.StringWriter

object PhenexDataSet {

  implicit val DataSetMarshaller = Marshaller.delegate[DataSet, String](MediaTypes.`application/xml`) { dataset =>
    val writer = new NeXMLWriter(UUID.randomUUID.toString)
    writer.setDataSet(dataset)
    val output = new StringWriter()
    writer.write(output)
    output.toString
  }

}