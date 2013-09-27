package org.phenoscape.kb;

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
//import scala.collection.JavaConversions._

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("myresource")
class MyResource {

  /**
   * Method handling HTTP GET requests. The returned object will be sent
   * to the client as "text/plain" media type.
   *
   * @return String that will be returned as a text/plain response.
   */
  @GET
  @Produces(Array(MediaType.TEXT_PLAIN))
  def getIt(): String = "Got it!"

}
